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
      with_transaction { |*targs| yield(*targs) }
    end

    def call
      event.start!
      with_transaction { |*targs| yield(*targs) }
    end

    private

    def with_transaction
      pipeline.schedule_cleanup

      destination, action, max_attempts_count =
        pipeline.find_transition(event.transition)
                .values_at(:to, :action, :attempts)

      storage.commit!(event)

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
      end
      raise
    ensure
      storage.commit!(event)
    end
  end
end
