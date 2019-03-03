module DirtyPipeline
  # Storage structure
  # {
  #   status: :errored,
  #   state: {
  #     field: "value",
  #   },
  #   errors: {
  #     "<task_id>": {
  #       error: "RuPost::API::Error",
  #       error_message: "Timeout error",
  #       created_at: 2018-01-01T13:22Z
  #     },
  #   },
  #   tasks: {
  #     <task_id>: {
  #       transition: "Create",
  #       args: ...,
  #       changes: ...,
  #       created_at: ...,
  #       updated_at: ...,
  #       attempts_count: 2,
  #     },
  #     <task_id>: {...},
  #   }
  # }
  module Redis
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

      def commit!(task)
        store["status"] = task.destination     if task.success?
        store["state"].merge!(task.changes)    unless task.changes.to_h.empty?

        error = {}
        error = task.error.to_h unless task.error.to_h.empty?
        store["errors"][task.id] = error

        data = {}
        data = task.data.to_h unless task.data.to_h.empty?
        store["tasks"][task.id] = data
        save!
      end

      def find_task(task_id)
        return unless (found_task = store.dig("tasks", task_id))
        Task.new(data: found_task, error: store.dig("errors", task_id))
      end

      private

      def valid_store?
        (store.keys & %w(status tasks errors state)).size.eql?(4)
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
            "tasks" => {},
            "errors" => {}
          }
        )
      end
    end
  end
end
