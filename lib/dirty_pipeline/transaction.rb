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
      event.start!
      with_transaction { |*targs| yield(*targs) }
    end

    def retry
      event.attempt_retry!
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

    def with_abort_handling
      return unless catch(:abort_transaction) { yield; nil }
      event.abort! unless event.failure?
      raise ActiveRecord::Rollback
    end
  end
end
