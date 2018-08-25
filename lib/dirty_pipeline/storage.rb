module DirtyPipeline
  class Storage
    FAILED_STATUS = "failed".freeze
    RETRY_STATUS = "retry".freeze
    PROCESSING_STATUS = "processing".freeze
    class InvalidPipelineStorage < StandardError; end

    attr_reader :subject, :field
    attr_accessor :store
    alias :to_h :store
    def initialize(subject, field)
      @subject = subject
      @field = field
      init_store(field)
    end

    def init_store(store_field)
      self.store = subject.send(store_field).to_h
      clear! if store.empty?
      return if valid_store?
      raise InvalidPipelineStorage, store
    end

    def valid_store?
      (
        store.keys &
          %w(status pipeline_status events errors state transaction_depth)
      ).size == 6
    end

    def clear
      self.store = subject.send(
        "#{field}=",
        "status" => nil,
        "pipeline_status" => nil,
        "state" => {},
        "events" => [],
        "errors" => [],
        "transaction_depth" => 1
      )
      DirtyPipeline.with_redis { |r| r.del(pipeline_status_key) }
    end

    def clear!
      clear
      commit!
    end

    def start!(transition, args)
      events << {
        "transition" => transition,
        "args" => args,
        "created_at" => Time.now,
        "cache" => {},
      }
      increment_attempts_count
      self.pipeline_status = PROCESSING_STATUS
      # self.status = "processing", should be set by Locker
      commit!
    end

    def start_retry!
      last_event.merge!(updated_at: Time.now)
      increment_attempts_count
      self.pipeline_status = PROCESSING_STATUS
      # self.status = "processing", should be set by Locker
      commit!
    end

    def complete!(output, destination)
      store["status"] = destination
      state.merge!(output)
      last_event.merge!(
        "output" => output,
        "updated_at" => Time.now,
        "success" => true,
      )
      commit!
    end

    def fail_event!
      fail_event
      commit!
    end

    def fail_event
      last_event["failed"] = true
    end

    def status
      store["status"]
    end

    def pipeline_status_key
      "pipeline-status:#{subject.class}:#{subject.id}:#{field}"
    end

    def pipeline_status=(value)
      DirtyPipeline.with_redis do |r|
        if value
          r.set(pipeline_status_key, value)
        else
          r.del(pipeline_status_key)
        end
      end
      store["pipeline_status"] = value
    end

    def commit_pipeline_status!(value = nil)
      self.pipeline_status = value
      last_event["cache"].clear
      commit!
    end
    alias :reset_pipeline_status! :commit_pipeline_status!

    def pipeline_status
      DirtyPipeline.with_redis do |r|
        store["pipeline_status"] = r.get(pipeline_status_key)
      end
      store.fetch("pipeline_status")
    end

    def state
      store.fetch("state")
    end

    def events
      store.fetch("events")
    end

    def last_event
      events.last.to_h
    end

    def last_event_error(event_idx = nil)
      event = events[event_idx] if event_idx
      event ||= last_event
      errors[event["error_idx"]].to_h
    end

    def errors
      store.fetch("errors")
    end

    def last_error
      errors.last.to_h
    end

    def reset_transaction_depth
      store["transaction_depth"] = 1
    end

    def reset_transaction_depth!
      reset_transaction_depth
      commit!
    end

    def transaction_depth
      store["transaction_depth"]
    end

    def increment_transaction_depth
      store["transaction_depth"] = store["transaction_depth"].to_i + 1
    end

    def increment_transaction_depth!
      increment_transaction_depth
      commit!
    end

    def increment_attempts_count
      last_event.merge!(
        "attempts_count" => last_event["attempts_count"].to_i + 1
      )
    end

    def increment_attempts_count!
      increment_attempts_count
      commit!
    end

    def save_retry(error)
      save_error(error)
      self.pipeline_status = RETRY_STATUS
    end

    def save_retry!(error)
      save_retry(error)
      commit!
    end

    def save_exception(exception)
      errors << {
        "error" => exception.class.to_s,
        "error_message" => exception.message,
        "created_at" => Time.current,
      }
      last_event["error_idx"] = errors.size - 1
      fail_event
      self.pipeline_status = FAILED_STATUS
    end

    def save_exception!(error)
      save_exception(error)
      commit!
    end

    def commit!
      subject.assign_attributes(field => store)
      subject.save!
    end

    def ready?
      storage.pipeline_status.nil?
    end

    def failed?
      pipeline_status == FAILED_STATUS
    end

    def processing?
      [PROCESSING_STATUS, RETRY_STATUS].include?(pipeline_status)
    end
  end
end
