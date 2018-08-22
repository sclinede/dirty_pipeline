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

    def self.call(*args, **kwargs)
      subject = args.shift
      instance = new(*args, **kwargs)
      instance.compensate(subject) if instance.respond_to?(:compensate)
      instance.call(subject)
    end
  end
end
