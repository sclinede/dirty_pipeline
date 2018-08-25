require "dirty_pipeline/version"

module DirtyPipeline
  require_relative "dirty_pipeline/locker.rb"
  require_relative "dirty_pipeline/storage.rb"
  require_relative "dirty_pipeline/transition.rb"
  require_relative "dirty_pipeline/status.rb"
  require_relative "dirty_pipeline/worker.rb"
  require_relative "dirty_pipeline/transaction.rb"
  require_relative "dirty_pipeline/base.rb"

  # This method should yield raw Redis connection
  def self.with_redis
    fail NotImplementedError
  end
end
