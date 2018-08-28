require 'sidekiq'
module DirtyPipeline
  class Worker
    include Sidekiq::Worker

    # args should contain - "enqueued_pipeline"
    # args should contain - some args to find_subject
    # args should contain - some args to make transition
    def perform(options)
      # FIXME: get rid of ActiveSupport #constantize
      pipeline_klass = options.fetch("enqueued_pipeline").constantize
      subject = pipeline_klass.find_subject(*options.fetch("find_subject_args"))
      transition, *targs = options.fetch("transition_args")
      case transition
      when "clean"
        pipeline_klass.new(subject).clean(transition, *targs)
      when "retry"
        pipeline_klass.new(subject).retry
      else
        pipeline_klass.new(subject).call(transition, *targs)
      end
    end
  end
end
