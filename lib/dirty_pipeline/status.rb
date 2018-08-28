module DirtyPipeline
  class Status
    attr_accessor :error, :succeeded
    attr_reader :storage, :pipeline

    def self.success(data)
      new(true, data)
    end

    def self.failure(data)
      new(false, data)
    end

    def initialize(success, data)
      @success = success
      @data = data
    end

    def success?
      !!succeeded
    end

    def failure?
      !success?
    end
  end
end
