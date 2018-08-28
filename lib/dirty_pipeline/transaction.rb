module DirtyPipeline
  class Transaction
    attr_reader :locker, :storage, :subject, :pipeline
    def initialize(pipeline)
      @pipeline = pipeline
      @storage = pipeline.storage
      @subject = pipeline.subject
    end

    def clean(*args)
      return unless in_progress?

      transition = storage.last_event["transition"]
      targs = storage.last_event["args"]

      return pipeline.schedule_cleanup(*targs) unless undoable?(transition)

      action = find_transition(transition).values_at(:action)
      action.undo(*targs)
      storage.finish_transition!(transition)
    rescue
      pipeline.schedule_cleanup(*targs)
      raise
    end

    def retry
      transition = storage.last_event["transition"]
      transition_args = storage.last_event["args"]

      return unless retryable?(transition)
      storage.start_retry!

      with_transaction(transition, *transition_args) { |*targs| yield(*targs) }
    end

    def call(*args)
      transition, *transition_args = args

      return if in_progress?(transition)
      storage.start!(transition, transition_args)
      pipeline.schedule_cleanup(*transition_args)

      with_transaction(transition, *transition_args) { |*targs| yield(*targs) }
    end

    private

    def with_transaction(transition, *args)
      begin
        destination, action, max_attempts_count =
          find_transition(transition).values_at(:to, :action, :attempts)

        # status.action_pool.unshift(action)
        subject.transaction(requires_new: true) do
          raise ActiveRecord::Rollback if catch(:abort_transaction) do
            yield(destination, action, *args); nil
          end
        end
      rescue => error
        next Retry(error) if try_again?(max_attempts_count)
        Exception(error)
        action.undo(*transition_args)
        storage.finish_transition!(transition)
        raise
      end
      storage.finish_transition!(transition)
    end

    def undoable?(transition)
      storage.transaction_queue.last == transition
    end

    def in_progress?(transition)
      storage.transaction_queue.size.positive? &&
        storage.transaction_queue.include?(transition)
    end

    def retryable?(transition)
      storage.status == Storage::RETRY_STATUS &&
        storage.transaction_queue.last == transition
    end

    def Retry(error, *args)
      storage.save_retry!(error)
      pipeline.schedule_retry
    end

    def Exception(error)
      storage.save_exception!(error)
      Status.error(error)
      pipeline.status.error = error
      pipeline.status.succeeded = false
    end

    def try_again?(max_attempts_count)
      return false unless max_attempts_count
      storage.last_event["attempts_count"].to_i < max_attempts_count
    end

    def find_transition(name)
      if (const_name = pipeline.class.const_get(name) rescue nil)
        name = const_name.to_s
      end
      pipeline.class.transitions_map.fetch(name.to_s).tap do |from:, **kwargs|
        next if from == Array(storage.status)
        next if from.include?(storage.status.to_s)
        raise InvalidTransition, "from `#{storage.status}` by `#{name}`"
      end
    end
  end
end
