module DirtyPipeline
  # Storage structure
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
  #       transition: "Create",
  #       args: ...,
  #       changes: ...,
  #       created_at: ...,
  #       updated_at: ...,
  #       attempts_count: 2,
  #     },
  #     <event_id>: {...},
  #   }
  # }
  class Storage
    class InvalidPipelineStorage < StandardError; end

    attr_reader :subject, :field, :store
    alias :to_h :store
    def initialize(subject, field)
      @subject = subject
      @field = field
      @store = subject.send(@field).to_h
      reset if @store.empty?
      raise InvalidPipelineStorage, store unless valid_store?
    end

    def reset!
      reset
      save!
    end

    def status
      store["status"]
    end

    def commit!(event)
      store["status"] = event.destination if event.destination
      store["state"].merge!(event.changes) unless event.changes.to_h.empty?
      store["errors"][event.id] = event.error unless event.error.to_h.empty?
      store["events"][event.id] = event.data unless event.data.to_h.empty?
      save!
    end

    def find_event(event_id)
      return unless (found_event = store.dig("events", event_id))
      Event.new(data: found_event, error: store.dig("errors", event_id))
    end

    private

    def valid_store?
      (store.keys & %w(status events errors state)).size.eql?(4)
    end

    # FIXME: save! - configurable method
    def save!
      subject.send("#{field}=", store)
      subject.save!
    end

    def reset
      @store = subject.send(
        "#{field}=",
        {
          "status" => nil,
          "state" => {},
          "events" => {},
          "errors" => {}
        }
      )
    end
  end
end
