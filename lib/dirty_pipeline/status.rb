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
      !!succeeded
    end

    def when_success(callback = nil)
      return self unless success?
      block_given? ? yield(self) : callback.(self)
      self
    end

    def when_failed(callback = nil)
      return self unless storage.failed?
      block_given? ? yield(self) : callback.(self)
      self
    end

    def errored?
      return if succeeded.nil?
      ready? && !succeeded
    end

    def when_error(callback = nil)
      return self unless errored?
      block_given? ? yield(self) : callback.(self)
      self
    end

    def ready?
      storage.pipeline_status.nil?
    end

    def when_processing(callback = nil)
      return self unless storage.processing?
      block_given? ? yield(self) : callback.(self)
      self
    end
  end
end
