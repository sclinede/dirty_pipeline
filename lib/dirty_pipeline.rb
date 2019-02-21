require "dirty_pipeline/version"
require "securerandom"
require "dry-initializer"

module DirtyPipeline
  require_relative "dirty_pipeline/ext/camelcase.rb"
  require_relative "dirty_pipeline/status.rb"
  require_relative "dirty_pipeline/worker.rb"
  require_relative "dirty_pipeline/transaction.rb"
  require_relative "dirty_pipeline/event.rb"

  # Redis
  require_relative "dirty_pipeline/redis/railway.rb"
  require_relative "dirty_pipeline/redis/storage.rb"
  require_relative "dirty_pipeline/redis/queue.rb"

  # Postgres
  require_relative "dirty_pipeline/pg.rb"
  require_relative "dirty_pipeline/pg/railway.rb"
  require_relative "dirty_pipeline/pg/storage.rb"
  require_relative "dirty_pipeline/pg/queue.rb"

  require_relative "dirty_pipeline/base.rb"
  require_relative "dirty_pipeline/transition.rb"

  # This method should yield raw Redis connection
  def self.with_redis
    fail NotImplementedError
  end

  # This method should yield raw PG connection
  def self.with_postgres
    fail NotImplementedError
  end

  def self.with_postgres_transaction
    with_postgres do |conn|
      conn.transaction do |transaction_conn|
        yield transaction_conn
      end
    end
  end

  # def self.with_postgres
  #   yield(ActiveRecord::Base.connection.raw_connection)
  # ensure
  #   ActiveRecord::Base.clear_active_connections!
  # end

  Queue = Redis::Queue
  Storage = Redis::Storage
  Railway = Redis::Railway

  def self.create!(conn)
    Queue.create!(conn)   if Queue.respond_to?(:create!)
    Storage.create!(conn) if Storage.respond_to?(:create!)
    Railway.create!(conn) if Railway.respond_to?(:create!)
  end

  def self.destroy!(conn)
    Queue.destroy!(conn)   if Queue.respond_to?(:destroy!)
    Storage.destroy!(conn) if Storage.respond_to?(:destroy!)
    Railway.destroy!(conn) if Railway.respond_to?(:destroy!)
  end
end
