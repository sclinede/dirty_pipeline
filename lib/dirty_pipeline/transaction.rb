module DirtyPipeline
  class Transaction
    attr_reader :locker, :storage, :subject, :pipeline, :event
    def initialize(pipeline, event)
      @pipeline = pipeline
      @subject = pipeline.subject
      @storage = pipeline.storage
      @event = event
    end

    def call
      pipeline.schedule_cleanup

      # Split attempts config and event dispatching
      destination, action, max_attempts_count =
        pipeline.find_transition(event.transition)
                .values_at(:to, :action, :attempts)

      storage.commit!(event)

      # FIXME: make configurable, now - hardcoded to AR API
      subject.transaction(requires_new: true) do
        with_abort_handling { yield(destination, action, *event.args) }
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

    private

    def with_abort_handling
      return unless catch(:abort_transaction) { yield; nil }
      event.abort! unless event.abort?
      raise ActiveRecord::Rollback
    end
  end
end
