module DirtyPipeline
  class Transition
    def Abort()
      throw :abort, true
    end

    def Error(error)
      throw :fail_with_error, error
    end

    def Success(after_commit: nil, **output)
      result = [output]
      result += Array(after_commit) if after_commit
      throw :success, result
    end

    def self.call(*args, **kwargs)
      subject = args.shift
      instance = new(*args, **kwargs)
      instance.call(subject)
    end
  end
end
