require "dirty_pipeline/version"

module DirtyPipeline
  require_relative "dirty_pipeline/ext/camelcase.rb"
  require_relative "dirty_pipeline/status.rb"
  require_relative "dirty_pipeline/storage.rb"
  require_relative "dirty_pipeline/worker.rb"
  require_relative "dirty_pipeline/transaction.rb"
  require_relative "dirty_pipeline/event.rb"
  require_relative "dirty_pipeline/queue.rb"
  require_relative "dirty_pipeline/railway.rb"
  require_relative "dirty_pipeline/base.rb"
  require_relative "dirty_pipeline/transition.rb"

  # This method should yield raw Redis connection
  def self.with_redis
    fail NotImplementedError
  end
end
