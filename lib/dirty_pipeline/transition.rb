module DirtyPipeline
  class Transition
    def Abort()
      throw :fail_operation, :abort
    end

    def Error(error)
      throw :fail_operation, error
    end

    def Success(changes = nil)
      throw :success, changes.to_h
    end

    def self.finalize(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, *args, **kwargs)
      pipeline.railway.switch_to(:call)
      return unless instance.respond_to?(:finalize)
      instance.finalize(pipeline.subject)
    end

    def self.undo(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, *args, **kwargs)
      return unless instance.respond_to?(:undo)
      instance.undo(pipeline.subject)
    end

    def self.call(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, *args, **kwargs)
      pipeline.railway[:undo] << event if instance.respond_to?(:undo)
      pipeline.railway[:finalize] << event if instance.respond_to?(:finalize)
      pipeline.railway.switch_to(:finalize) if instance.respond_to?(:finalize)
      new(event, *args, **kwargs).call(pipeline.subject)
    end

    attr_reader :event
    def initialize(event, *, **)
      @event = event
    end

    def fetch(key)
      event.cache.fetch(key) { event.cache[key] = yield }
    end
  end
end
