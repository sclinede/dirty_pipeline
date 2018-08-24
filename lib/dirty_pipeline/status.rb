module DirtyPipeline
  class Status < SimpleDelegator
    attr_accessor :error, :succeeded, :action_pool
    attr_reader :storage, :pipeline
    def initialize(*)
      super
      @storage = __getobj__.storage
      @action_pool = []
    end

    def wrap
      return self if succeeded == false
      self.succeeded = nil
      yield
      self
    end

    def success?
      succeeded
    end

    def when_success(callback = nil)
      return self unless success?
      if block_given?
        yield(self)
      else
        callback.call(self)
      end
      self
    end

    def when_failed(callback = nil)
      return self unless storage.failed?
      if block_given?
        yield(self)
      else
        callback.call(self)
      end
      self
    end

    def errored?
      return if succeeded.nil?
      ready? && !succeeded
    end

    def when_error(callback = nil)
      return self unless errored?
      if block_given?
        yield(self)
      else
        callback.call(self)
      end
      self
    end

    def ready?
      storage.pipeline_status.nil?
    end

    def when_processing(callback = nil)
      return self unless storage.processing?
      if block_given?
        yield(self)
      else
        callback.call(self)
      end
      self
    end
  end
end
