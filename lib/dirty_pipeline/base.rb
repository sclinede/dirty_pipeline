module DirtyPipeline
  class Base
    DEFAULT_RETRY_DELAY = 5 * 60 # 5 minutes
    DEFAULT_CLEANUP_DELAY = 60 * 60 * 24 # 1 day
    RESERVED_STATUSES = [
      Storage::FAILED_STATUS,
      Storage::PROCESSING_STATUS,
      Storage::RETRY_STATUS,
      Locker::CLEAN,
    ]

    class ReservedStatusError < StandardError; end
    class InvalidTransition < StandardError; end

    class << self
      def find_subject(*args)
        fail NotImplemented
      end

      attr_reader :transitions_map
      def inherited(child)
        child.instance_variable_set(:@transitions_map, Hash.new)
      end
      # PG JSONB column
      # {
      #   status: :errored,
      #   state: {
      #     field: "value",
      #   },
      #   errors: [
      #     {
      #       error: "RuPost::API::Error",
      #       error_message: "Timeout error",
      #       created_at: 2018-01-01T13:22Z
      #     },
      #   ],
      #   events: [
      #     {
      #       action: Init,
      #       input: ...,
      #       created_at: ...,
      #       updated_at: ...,
      #       attempts_count: 2,
      #     },
      #     {...},
      #   ]
      # }
      attr_accessor :pipeline_storage, :retry_delay, :cleanup_delay

      def transition(action, from:, to:, name: action.to_s, attempts: 1)
        raise ReservedStatusError unless valid_statuses?(from, to)
        @transitions_map[name] = {
          action: action,
          from: Array(from).map(&:to_s),
          to: to.to_s,
          attempts: attempts,
        }
      end

      private

      def valid_statuses?(from, to)
        ((Array(to) + Array(from)) & RESERVED_STATUSES).empty?
      end
    end

    def enqueue(transition_name, *args)
      Shipping::PipelineWorker.perform_async(
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "transition_args" => args.unshift(transition_name),
      )
    end

    def reset!
      storage.reset_pipeline_status!
    end

    def clear!
      storage.clear!
    end

    def cache
      storage.store["cache"]
    end

    attr_reader :subject, :error, :storage, :status
    def initialize(subject)
      @subject = subject
      @storage = Storage.new(subject, self.class.pipeline_storage)
      @locker = Locker.new(@subject, @storage)
      @status = Status.new(self)
    end

    def call(*args)
      Result() do
        after_commit = nil
        # transaction with support of external calls
        transaction(*args) do |destination, action, *targs|
          output = {}
          fail_cause = nil

          output, *after_commit = catch(:success) do
            fail_cause = catch(:fail_with_error) do
              Abort() if catch(:abort) do
                throw :success, action.(self, *targs)
              end
            end
            nil
          end

          if fail_cause
            ExpectedError(fail_cause)
          else
            Success(destination, output)
          end
        end

        Array(after_commit).each { |cb| cb.call(subject) } if after_commit
      end
    end

    private

    attr_reader :locker

    def find_subject_args
      subject.id
    end

    def retry_delay
      self.class.retry_delay || DEFAULT_RETRY_DELAY
    end

    def cleanup_delay
      self.class.cleanup_delay || DEFAULT_CLEANUP_DELAY
    end

    def Result()
      status.wrap { yield }
    end

    def Retry(error, *args)
      storage.save_retry!(error)
      Shipping::PipelineWorker.perform_in(
        retry_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "retry" => true,
      )
    end

    def ExpectedError(cause)
      status.error = cause
      storage.fail_event!
      status.succeeded = false
    end

    def Exception(error)
      storage.save_exception!(error)
      status.error = error
      status.succeeded = false
    end

    def Abort()
      status.succeeded = false
      throw :abort_transaction, true
    end

    def Success(destination, output)
      cache.clear
      storage.complete!(output, destination)
      status.succeeded = true
    end

    def try_again?(max_attempts_count)
      return unless max_attempts_count
      storage.last_event["attempts_count"].to_i < max_attempts_count
    end

    def find_transition(name)
      if (const_name = self.class.const_get(name) rescue nil)
        name = const_name.to_s
      end
      self.class.transitions_map.fetch(name.to_s).tap do |from:, **kwargs|
        next if from == Array(storage.status)
        next if from.include?(storage.status.to_s)
        raise InvalidTransition, "from `#{storage.status}` by `#{name}`"
      end
    end

    def schedule_cleanup
      Shipping::PipelineWorker.perform_in(
        cleanup_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "transition_args" => [Locker::CLEAN],
      )
    end

    def transaction(*args)
      locker.with_lock(*args) do |transition, *transition_args|
        begin
          schedule_cleanup
          destination, action, max_attempts_count =
            find_transition(transition).values_at(:to, :action, :attempts)

          status.action_pool.unshift(action)
          subject.transaction(requires_new: true) do
            raise ActiveRecord::Rollback if catch(:abort_transaction) do
              yield(destination, action, *transition_args); nil
            end
          end
        rescue => error
          if try_again?(max_attempts_count)
            Retry(error)
          else
            Exception(error)
          end
          raise
        ensure
          if status.succeeded == false
            status.action_pool.each do |reversable_action|
              next unless reversable_action.respond_to?(:undo)
              reversable_action.undo(self, *transition_args)
            end
          end
        end
      end
    end
  end
end
