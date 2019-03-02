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
      action, max_attempts_count =
        pipeline.find_transition!(event).values_at(:action, :attempts)

      storage.commit!(event)

      # FIXME: make configurable, now - hardcoded to AR API
      # also, make sure, that we need transaction here
      # subject.transaction(requires_new: true) do
      # subject.transaction do
        with_abort_handling { yield(action, *event.args) }
      # end
    rescue => exception
      event.link_exception(exception)
      if max_attempts_count.to_i > event.attempts_count
        event.retry!
        pipeline.schedule_retry
      else
        pipeline.reset!
      end
      raise
    ensure
      storage.commit!(event)
    end

    private

    def with_abort_handling
      return unless catch(:abort_transaction) { yield; nil }
      event.abort! unless event.abort?
      # temporary turned off, due to question about transaction
      # raise ActiveRecord::Rollback
    end
  end
end
