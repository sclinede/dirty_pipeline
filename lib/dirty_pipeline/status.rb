module DirtyPipeline
  class Status
    attr_reader :success, :tag, :data

    def self.success(data, tag: :success)
      new(true, data, tag)
    end

    def self.failure(data, tag: :exception)
      new(false, data, tag)
    end

    def initialize(success, data, tag = nil)
      @success = success
      @data = data
      @tag = tag
    end

    def success?
      !!success
    end

    def failure?
      !success?
    end
  end
end
