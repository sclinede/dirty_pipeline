module DirtyPipeline
  module PG
    class Queue
      # decoder = PG::TextDecoder::Array.new
      # see https://stackoverflow.com/questions/34886260/how-do-you-decode-a-json-field-using-the-pg-gem
      def self.create!(connection)
        connection.exec <<~SQL
          ALTER TABLE IF EXISTS dp_active_events RENAME TO dp_active_tasks;
          CREATE TABLE IF NOT EXISTS dp_active_tasks (
            key TEXT CONSTRAINT primary_task_queues_key PRIMARY KEY,
            payload TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );

          ALTER TABLE IF EXISTS dp_event_queues RENAME TO dp_task_queues;
          CREATE SEQUENCE IF NOT EXISTS dp_event_queues_id_seq START 1;
          CREATE SEQUENCE IF NOT EXISTS dp_task_queues_id_seq START nextval('dp_event_queues_id_seq');
          CREATE TABLE IF NOT EXISTS dp_task_queues (
            id BIGINT PRIMARY KEY DEFAULT nextval('dp_task_queues_id_seq'),
            key TEXT NOT NULL,
            payload TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );
        SQL
      end

      def self.destroy!(connection)
        connection.exec <<~SQL
          DROP TABLE IF EXISTS dp_active_tasks;
          DROP TABLE IF EXISTS dp_task_queues;
          DROP SEQUENCE IF EXISTS dp_task_queues_id_seq;
        SQL
      end

      def initialize(operation, subject_class, subject_id, transaction_id)
        @root = "dirty-pipeline-queue:#{subject_class}:#{subject_id}:" \
                "op_#{operation}:txid_#{transaction_id}"
      end

      def with_postgres(&block)
        DirtyPipeline.with_postgres(&block)
      end

      DELETE_ACTIVE = <<~SQL
        DELETE FROM dp_active_tasks WHERE key = $1;
      SQL
      DELETE_TASKS = <<~SQL
        DELETE FROM dp_task_queues WHERE key = $1;
      SQL

      def clear!
        with_postgres do |c|
          c.transaction do |tc|
            tc.exec(DELETE_ACTIVE, [active_task_key])
            tc.exec(DELETE_TASKS, [tasks_queue_key])
          end
        end
      end

      SELECT_ALL_TASKS = <<~SQL
        SELECT payload FROM dp_task_queues WHERE key = $1 ORDER BY id DESC;
      SQL
      def to_a
        with_postgres do |c|
          c.exec(SELECT_ALL_TASKS, [tasks_queue_key]).to_a.map! do |row|
            Task.unpack(row.values.first)
          end
        end
      end

      PUSH_TASK = <<~SQL
        INSERT INTO dp_task_queues (id, key, payload)
        VALUES (-nextval('dp_task_queues_id_seq'), $1, $2);
      SQL
      def push(task)
        with_postgres do |c|
          c.exec(PUSH_TASK, [tasks_queue_key, task.to_json])
        end

        self
      end
      alias :<< :push

      UNSHIFT_TASK = <<~SQL
        INSERT INTO dp_task_queues (key, payload) VALUES ($1, $2);
      SQL
      def unshift(task)
        with_postgres do |c|
          c.exec(UNSHIFT_TASK, [tasks_queue_key, task.to_json])
        end
        self
      end

      SELECT_LAST_TASK = <<~SQL
        SELECT id, payload FROM dp_task_queues
        WHERE key = $1
        ORDER BY id DESC LIMIT 1;
      SQL
      DELETE_TASK = <<~SQL
        DELETE FROM dp_task_queues WHERE key = $1 AND id = $2;
      SQL
      DELETE_ACTIVE_TASK = <<~SQL
        DELETE FROM dp_active_tasks WHERE key = $1;
      SQL
      SET_TASK_ACTIVE = <<~SQL
        INSERT INTO dp_active_tasks (key, payload) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload;
      SQL
      def pop
        with_postgres do |c|
          c.transaction do |tc|
            task_id, raw_task =
              PG.multi(tc.exec(SELECT_LAST_TASK, [tasks_queue_key]))
            if raw_task.nil?
              tc.exec(DELETE_ACTIVE_TASK, [active_task_key])
            else
              tc.exec(DELETE_TASK, [tasks_queue_key, task_id])
              tc.exec(SET_TASK_ACTIVE, [active_task_key, raw_task])
            end
            Task.unpack(raw_task)
          end
        end
      end

      SELECT_ACTIVE_TASK = <<~SQL
        SELECT payload FROM dp_active_tasks WHERE key = $1;
      SQL
      def processing_task
        with_postgres do |c|
          raw_task = PG.single(
            c.exec(SELECT_ACTIVE_TASK, [active_task_key])
          )
          Task.unpack(raw_task)
        end
      end

      private

      def tasks_queue_key
        "#{@root}:tasks"
      end

      def active_task_key
        "#{@root}:active"
      end
    end
  end
end
