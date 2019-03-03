module DirtyPipeline
  class Transaction
    attr_reader :locker, :storage, :subject, :pipeline, :task
    def initialize(pipeline, task)
      @pipeline = pipeline
      @subject = pipeline.subject
      @storage = pipeline.storage
      @task = task
    end

    def call
      pipeline.schedule_cleanup

      # Split attempts config and action dispatching
      action, max_attempts_count =
        pipeline.find_transition!(task).values_at(:action, :attempts)

      storage.commit!(task)

      # make sure, that we need transaction here
      # subject.transaction do
      with_abort_handling { yield(action, *task.args) }
      # end
    rescue => exception
      task.link_exception(exception)
      if max_attempts_count.to_i > task.attempts_count
        task.retry!
        pipeline.schedule_retry
      else
        pipeline.reset!
      end
      raise
    ensure
      storage.commit!(task)
    end

    private

    def with_abort_handling
      return unless catch(:abort_transaction) { yield; nil }
      task.abort! unless task.abort?
      # temporary turned off, due to question about transaction
      # raise ActiveRecord::Rollback
    end
  end
end
