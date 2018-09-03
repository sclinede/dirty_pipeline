require "bundler/setup"
require "dirty_pipeline"

require "dotenv"
Dotenv.load(".env.test")
require "timecop"

require_relative "./support/infrastructure"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) { DB.reset! }

  module DirtyPipeline
    def self.redis
      Thread.current[:dirty_redis] ||= Redis.new(redis_url: ENV["REDIS_URL"])
    end

    def self.with_redis
      yield(redis)
    end
  end
end
