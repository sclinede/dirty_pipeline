module DirtyPipeline
  # Storage structure
  # {
  #   status: :errored,
  #   state: {
  #     field: "value",
  #   }
  # }
  module PG
    class Storage
      class InvalidPipelineStorage < StandardError; end

      def self.create!(connection)
        connection.exec <<~SQL
          ALTER TABLE IF EXISTS dp_events_store RENAME TO dp_tasks_store;
          CREATE TABLE IF NOT EXISTS dp_tasks_store (
            uuid TEXT CONSTRAINT primary_active_operations_key PRIMARY KEY,
            context TEXT NOT NULL,
            data TEXT,
            error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );
        SQL
      end

      def self.destroy!(connection)
        connection.exec <<~SQL
          DROP TABLE IF EXISTS dp_tasks_store;
        SQL
      end

      attr_reader :subject, :field, :store, :subject_key
      alias :to_h :store
      def initialize(subject, field)
        @subject = subject
        @field = field
        @store = subject.send(@field).to_h
        reset if @store.empty?
        @subject_key = "#{subject.class}:#{subject.id}"
        raise InvalidPipelineStorage, store unless valid_store?
      end

      def with_postgres(&block)
        DirtyPipeline.with_postgres(&block)
      end

      def reset!
        reset
        save!
      end

      def status
        store["status"]
      end

      SAVE_TASK = <<~SQL
        INSERT INTO dp_tasks_store (uuid, context, data, error)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (uuid)
        DO UPDATE SET data = EXCLUDED.data, error = EXCLUDED.error;
      SQL
      def commit!(task)
        store["status"] = task.destination  if task.success?
        store["state"].merge!(task.changes) unless task.changes.to_h.empty?
        data, error = {}, {}
        data = task.data.to_h if task.data.respond_to?(:to_h)
        error = task.error.to_h if task.error.respond_to?(:to_h)
        with_postgres do |c|
          c.exec(
            SAVE_TASK,
            [task.id, subject_key, JSON.dump(data), JSON.dump(error)]
          )
        end
        save!
      end

      FIND_TASK = <<-SQL
        SELECT data, error FROM dp_tasks_store
        WHERE uuid = $1 AND context = $2;
      SQL
      def find_task(task_id)
        with_postgres do |c|
          found_task, found_error =
            PG.multi(c.exec(FIND_TASK, [task_id, subject_key]))
          return unless found_task
          Task.new(
            data: JSON.parse(found_task), error: JSON.parse(found_error)
          )
        end
      end

      private

      def valid_store?
        (store.keys & %w(status state)).size.eql?(2)
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
          }
        )
      end
    end
  end
end
