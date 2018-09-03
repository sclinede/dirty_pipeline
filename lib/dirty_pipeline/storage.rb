module DirtyPipeline
  class Storage
    SUCCESS_STATUS = "success".freeze
    FAILURE_STATUS = "failure".freeze
    RETRY_STATUS = "retry".freeze
    PROCESSING_STATUS = "processing".freeze
    class InvalidPipelineStorage < StandardError; end

    attr_reader :subject, :field, :transactions_queue
    attr_accessor :store
    alias :to_h :store
    def initialize(subject, field)
      @subject = subject
      @field = field
      init_store(field)
    end

    def init_store(store_field)
      self.store = subject.send(store_field).to_h
      clear if store.empty?
      return if valid_store?
      raise InvalidPipelineStorage, store
    end

    def valid_store?
      (store.keys & %w(status events errors state)).size.eql?(4)
    end

    # PG JSONB column
    # {
    #   status: :errored,
    #   state: {
    #     field: "value",
    #   },
    #   errors: {
    #     "<event_id>": {
    #       error: "RuPost::API::Error",
    #       error_message: "Timeout error",
    #       created_at: 2018-01-01T13:22Z
    #     },
    #   },
    #   events: {
    #     <event_id>: {
    #       action: Init,
    #       input: ...,
    #       created_at: ...,
    #       updated_at: ...,
    #       attempts_count: 2,
    #     },
    #     <event_id>: {...},
    #   }
    # }
    def clear
      self.store = subject.send(
        "#{field}=",
        "status" => nil,
        "state" => {},
        "events" => {},
        "errors" => {}
      )
    end

    def clear!
      clear
      subject.update_attributes!(field => store)
    end

    def status
      store["status"]
    end

    def commit!(event)
      store["status"] = event.destination if event.destination
      store["state"].merge!(event.changes) unless event.changes.to_h.empty?
      store["errors"][event.id] = event.error unless event.error.to_h.empty?
      store["events"][event.id] = event.data unless event.data.to_h.empty?
      subject.assign_attributes(field => store)
      subject.save!
    end

    def processing_event
      find_event(transactions_queue.processing_event.id)
    end

    def find_event(event_id)
      return unless (found_event = store["events"][event_id])
      Event.new(data: found_event, error: store["errors"][event_id])
    end
  end
end
