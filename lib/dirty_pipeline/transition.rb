module DirtyPipeline
  class Transition
    def Abort()
      throw :abort, true
    end

    def Error(error)
      throw :fail_with_error, error
    end

    def Success(output = nil, after_commit: nil, &block)
      result = [output.to_h]
      after_commit = Array(after_commit) << block if block_given?
      result += Array(after_commit) if after_commit
      throw :success, result
    end

    def self.undo(*args, **kwargs)
      pipeline = args.shift
      instance = new(pipeline, *args, **kwargs)
      return unless instance.respond_to?(:undo)
      instance.undo(pipeline.subject)
    end

    def self.call(*args, **kwargs)
      pipeline = args.shift
      new(pipeline, *args, **kwargs).call(pipeline.subject)
    end

    attr_reader :pipeline
    def initialize(pipeline, *, **)
      @pipeline = pipeline
    end

    def fetch(key)
      pipeline.cache.fetch(key) { pipeline.cache[key] = yield }
    end
  end
end
