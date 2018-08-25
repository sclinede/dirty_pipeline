module DirtyPipeline
  class Worker
    include Sidekiq::Worker

    sidekiq_options queue: "default",
                    retry: 1,
                    dead: true

    # args should contain - "enqueued_pipelines" - Array of Pipeline children
    # args should contain - some args to find_subject
    # args should contain - some args to make transition

    def perform(options)
      # FIXME: get rid of ActiveSupport #constantize
      pipeline_klass = options.fetch("enqueued_pipeline").constantize
      subject = pipeline_klass.find_subject(*options.fetch("find_subject_args"))
      pipeline_klass.new(subject).call(*options.fetch("transition_args"))
    end
  end
end
