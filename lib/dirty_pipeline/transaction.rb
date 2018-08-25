module DirtyPipeline
  class Transaction
    attr_reader :locker, :storage, :subject, :pipeline
    def initialize(pipeline)
      @pipeline = pipeline
      @storage = pipeline.storage
      @subject = pipeline.subject
      @locker = Locker.new(@subject, @storage)
    end

    def call(*args)
      locker.with_lock(*args) do |transition, *transition_args|
        pipeline.schedule_cleanup
        begin
          destination, action, max_attempts_count =
            find_transition(transition).values_at(:to, :action, :attempts)

          # status.action_pool.unshift(action)
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
          unless pipeline.status.success?
            storage.events
                   .last(storage.transaction_depth)
                   .reverse
                   .each do |params|
              transition = params["transition"]
              targs = params["args"]
              reversable_action = find_transition(transition).fetch(:action)
              reversable_action.undo(self, *targs)
            end
          end
        end
      end
    end

    private

    def Retry(error, *args)
      storage.save_retry!(error)
      pipeline.schedule_retry
    end

    def Exception(error)
      storage.save_exception!(error)
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
