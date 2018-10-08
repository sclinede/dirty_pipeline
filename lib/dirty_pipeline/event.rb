require 'json'

module DirtyPipeline
  class Event
    NEW = "new".freeze
    START = "started".freeze
    FAILURE = "failed".freeze
    ABORT = "aborted".freeze
    RETRY = "retry".freeze
    SUCCESS = "succeeded".freeze

    def self.create(transition, *args, tx_id:, **kwargs)
      args << kwargs unless kwargs.empty?
      new(
        data: {
          # FIXME: SecureRandom
          "uuid" => SecureRandom.uuid,
          "transaction_uuid" => tx_id,
          "transition" => transition,
          "args" => args,
        }
      )
    end

    attr_reader :id, :tx_id, :error, :data
    def initialize(options = {}, data: nil,  error: nil)
      unless options.empty?
        options_hash = options.to_h
        data  ||= options_hash["data"]
        error ||= options_hash["error"]
      end

      data_hash = data.to_h

      @tx_id     = data_hash.fetch("transaction_uuid")
      @id        = data_hash.fetch("uuid")
      transition = data_hash.fetch("transition")
      args       = data_hash.fetch("args").to_a
      @data = {
        "uuid" => @id,
        "transaction_uuid" => @tx_id,
        "transition" => transition,
        "args" => args,
        "created_at" => Time.now,
        "cache" => {},
        "attempts_count" => 1,
        "status" => NEW,
      }.merge(data_hash)
      @error = error
    end

    def to_h
      {data: @data, error: @error}
    end

    %w(args transition cache destination changes).each do |method_name|
      define_method("#{method_name}") { @data[method_name] }
    end

    %w(new start retry failure success abort).each do |method_name|
      define_method("#{method_name}?") do
        @data["status"] == self.class.const_get(method_name.upcase)
      end

      define_method("#{method_name}!") do
        @data["status"] = self.class.const_get(method_name.upcase)
      end
    end

    def link_exception(exception)
      @error = {
        "exception" => exception.class.to_s,
        "exception_message" => exception.message,
        "created_at" => Time.now,
      }
      failure!
    end

    def attempts_count
      @data["attempts_count"].to_i
    end

    def attempt_retry!
      @data["updated_at"] = Time.now
      @data["attempts_count"] = attempts_count + 1
    end

    def complete(changes, destination)
      @data.merge!(
        "destination" => destination,
        "changes" => changes,
        "updated_at" => Time.now,
        "status" => SUCCESS,
      )
    end
  end
end
