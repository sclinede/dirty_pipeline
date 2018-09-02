require 'sidekiq'
module DirtyPipeline
  class Worker
    include Sidekiq::Worker
    using StringCamelcase

    # args should contain - "enqueued_pipeline"
    # args should contain - some args to find_subject
    # args should contain - some args to make transition
    def perform(options = {})
      pipeline_klass =
        Kernel.const_get(options.fetch("enqueued_pipeline").to_s.camelcase)
      subject = pipeline_klass.find_subject(*options.fetch("find_subject_args"))
      transaction_id = options.fetch("transaction_id")
      pipeline = pipeline_klass.new(subject, uuid: transaction_id)
      operation = options.fetch("operation")

      case operation
      when "cleanup"
        pipeline.clean
      when "retry"
        return pipeline.retry
      end
      pipeline.call
    end
  end
end
