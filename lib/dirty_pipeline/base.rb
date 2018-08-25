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

    attr_reader :subject, :error, :storage, :status, :transitions_chain
    def initialize(subject)
      @subject = subject
      @storage = Storage.new(subject, self.class.pipeline_storage)
      @status = Status.new(self)
      @transitions_chain = []
    end

    def enqueue(transition_name, *args)
      DirtyPipeline::Worker.perform_async(
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
      storage.last_event["cache"]
    end

    def chain(*args)
      transitions_chain << args
      self
    end

    def execute
      Result() do
        transitions_chain.each do |targs|
          call(*targs)
          storage.increment_transaction_depth!
        end
        storage.reset_transaction_depth!
        transitions_chain.clear
      end
    end

    def call(*args)
      storage.reset_transaction_depth! if transitions_chain.empty?
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
            Failure(fail_cause)
          else
            Success(destination, output)
          end
        end

        Array(after_commit).each { |cb| cb.call(subject) } if after_commit
      end
    end

    def schedule_retry
      ::DirtyPipeline::Worker.perform_in(
        retry_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "retry" => true,
      )
    end

    def schedule_cleanup
      ::DirtyPipeline::Worker.perform_in(
        cleanup_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "transition_args" => [Locker::CLEAN],
      )
    end

    private

    def find_subject_args
      subject.id
    end

    def retry_delay
      self.class.retry_delay || DEFAULT_RETRY_DELAY
    end

    def cleanup_delay
      self.class.cleanup_delay || DEFAULT_CLEANUP_DELAY
    end

    def transaction(*args)
      ::DirtyPipeline::Transaction.new(self).call(*args) do |*targs|
        yield(*targs)
      end
    end

    def Result()
      status.wrap { yield }
    end

    def Failure(cause)
      storage.fail_event!
      status.error = cause
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
  end
end
