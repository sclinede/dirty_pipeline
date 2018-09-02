module DirtyPipeline
  class Transaction
    attr_reader :locker, :storage, :subject, :pipeline, :queue, :event
    def initialize(pipeline, queue, event)
      @pipeline = pipeline
      @subject = pipeline.subject
      @storage = pipeline.storage
      @queue = queue
      @event = event
    end

    def retry
      event.attempt_retry!
      pipeline.schedule_cleanup

      with_transaction { |*targs| yield(*targs) }
    end

    def call
      # return unless queue.event_in_progress?(event)

      event.start!
      pipeline.schedule_cleanup

      with_transaction { |*targs| yield(*targs) }
    end

    private

    def with_transaction
      destination, action, max_attempts_count =
        pipeline.find_transition(event.transition)
                .values_at(:to, :action, :attempts)

      storage.commit!(event)

      # status.action_pool.unshift(action)
      subject.transaction(requires_new: true) do
        raise ActiveRecord::Rollback if catch(:abort_transaction) do
          yield(destination, action, *event.args); nil
        end
      end
    rescue => exception
      event.link_exception(exception)
      if max_attempts_count.to_i > event.attempts_count
        event.retry!
        pipeline.schedule_retry
      else
        pipeline.schedule_cleanup
      end
      raise
    ensure
      storage.commit!(event)
    end
  end
end
