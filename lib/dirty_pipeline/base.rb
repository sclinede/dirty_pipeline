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
      @status = Status.success(subject)
      @transitions_chain = []
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
      transition_args = transitions_chain.shift
      return status unless transition_args
      if status.success?
        call(*transition_args, &method(:execute))
      else
        execute
      end
    end

    def retry
      transaction.retry
    end

    def clean(*args)
      transaction.clean(*args)
    end

    def call(*args)
      after_commit = nil
      transaction.call(*args) do |destination, action, *targs|
        output = {}
        fail_cause = nil

        output, *after_commit = catch(:success) do
          fail_cause = catch(:fail_with_error) do
            Abort() if catch(:abort) do
              throw :success, action.call(self, *targs)
            end
          end
          nil
        end

        if fail_cause
          Failure(fail_cause)
        else
          Success(destination, output)
        end

        yield if block_given?
      end

      Array(after_commit).each { |cb| cb.call(subject) } if after_commit
      status
    end

    def schedule_cleanup(*args)
      ::DirtyPipeline::Worker.perform_in(
        cleanup_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "transition_args" => args.unshift("clean"),
      )
    end

    def schedule_retry
      ::DirtyPipeline::Worker.perform_in(
        retry_delay,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "transition_args" => ["retry"],
      )
    end

    def when_success
      yield(self) if status.success?
      self
    end

    def when_failure
      yield(self) if status.failure?
      self
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

    def transaction
      ::DirtyPipeline::Transaction.new(self)
    end

    def Failure(cause)
      storage.fail_event!
      self.status = Result.error(cause)
    end

    def Abort()
      self.status = Result.failure(:aborted)
      throw :abort_transaction, true
    end

    def Success(destination, output)
      cache.clear
      storage.complete!(output, destination)
      self.status = Result.success(subject)
    end
  end
end
