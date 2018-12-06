module DirtyPipeline
  class Rollback < StandardError; end

  class Transition
    def Failure(error)
      railway&.switch_to(:undo)
      throw :fail_transition, error
    end

    def Success(changes = nil)
      case railway&.active
      when "finalize_undo"
        railway&.switch_to(:undo)
      when "undo"
        railway&.switch_to(:finalize_undo) if respond_to?(:finalize_undo)
      when "call"
        railway&.switch_to(:finalize) if respond_to?(:finalize)
      when "finalize"
        railway&.switch_to(:call)
      end
      throw :success, changes.to_h
    end

    def self.finalize_undo(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      return unless instance.respond_to?(:finalize_undo)
      instance.finalize_undo(pipeline.subject)
    end

    def self.finalize(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      return unless instance.respond_to?(:finalize)
      instance.finalize(pipeline.subject)
    end

    def self.undo(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      pipeline&.railway&.send(:[], :finalize_undo)&.send(:<<, event)
      return unless instance.respond_to?(:undo)
      instance.undo(pipeline.subject)
    end

    def self.call(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      pipeline&.railway&.send(:[], :finalize)&.send(:<<, event)
      prepare_undo(pipeline, event)
      instance.call(pipeline.subject)
    end

    def self.prepare_undo(pipeline, event)
      anti_event = event.dup
      anti_event.source, anti_event.destination =
        event.destination, event.source
      pipeline&.railway&.send(:[], :undo)&.send(:unshift, anti_event)
    end

    attr_reader :event, :railway
    def initialize(event, railway, *, **)
      @event = event
      @railway = railway
    end

    def cache(key)
      event.cache.fetch(key) { event.cache[key] = yield }
    end
  end
end
