module DirtyPipeline
  module PG
    class Queue
      # decoder = PG::TextDecoder::Array.new
      # see https://stackoverflow.com/questions/34886260/how-do-you-decode-a-json-field-using-the-pg-gem
      def self.create!(connection)
        connection.exec <<~SQL
          CREATE TABLE dp_active_events (
            key TEXT CONSTRAINT primary_event_queues_key PRIMARY KEY,
            payload TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );

          CREATE SEQUENCE dp_event_queues_id_seq START 1;
          CREATE TABLE dp_event_queues (
            id BIGINT PRIMARY KEY DEFAULT nextval('dp_event_queues_id_seq'),
            key TEXT NOT NULL,
            payload TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );
        SQL
      end

      def self.destroy!(connection)
        connection.exec <<~SQL
          DROP TABLE IF EXISTS dp_active_events;
          DROP TABLE IF EXISTS dp_event_queues;
          DROP SEQUENCE IF EXISTS dp_event_queues_id_seq;
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
        DELETE FROM dp_active_events WHERE key = $1;
      SQL
      DELETE_EVENTS = <<~SQL
        DELETE FROM dp_event_queues WHERE key = $1;
      SQL

      def clear!
        with_postgres do |c|
          c.transaction do |tc|
            tc.exec(DELETE_ACTIVE, [active_event_key])
            tc.exec(DELETE_EVENTS, [events_queue_key])
          end
        end
      end

      SELECT_ALL_EVENTS = <<~SQL
        SELECT payload FROM dp_event_queues WHERE key = $1 ORDER BY id DESC;
      SQL
      def to_a
        with_postgres do |c|
          c.exec(SELECT_ALL_EVENTS, [events_queue_key]).to_a.map! do |row|
            unpack(row.values.first)
          end
        end
      end

      PUSH_EVENT = <<~SQL
        INSERT INTO dp_event_queues (id, key, payload)
        VALUES (-nextval('dp_event_queues_id_seq'), $1, $2);
      SQL
      def push(event)
        with_postgres do |c|
          c.exec(PUSH_EVENT, [events_queue_key, pack(event)])
        end

        self
      end
      alias :<< :push

      UNSHIFT_EVENT = <<~SQL
        INSERT INTO dp_event_queues (key, payload) VALUES ($1, $2);
      SQL
      def unshift(event)
        with_postgres do |c|
          c.exec(UNSHIFT_EVENT, [events_queue_key, pack(event)])
        end
        self
      end

      SELECT_LAST_EVENT = <<~SQL
        SELECT id, payload FROM dp_event_queues
        WHERE key = $1
        ORDER BY id DESC LIMIT 1;
      SQL
      DELETE_EVENT = <<~SQL
        DELETE FROM dp_event_queues WHERE key = $1 AND id = $2;
      SQL
      DELETE_ACTIVE_EVENT = <<~SQL
        DELETE FROM dp_active_events WHERE key = $1;
      SQL
      SET_EVENT_ACTIVE = <<~SQL
        INSERT INTO dp_active_events (key, payload) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload;
      SQL
      def pop
        with_postgres do |c|
          c.transaction do |tc|
            event_id, raw_event =
              PG.multi(tc.exec(SELECT_LAST_EVENT, [events_queue_key]))
            if raw_event.nil?
              tc.exec(DELETE_ACTIVE_EVENT, [active_event_key])
            else
              tc.exec(DELETE_EVENT, [events_queue_key, event_id])
              tc.exec(SET_EVENT_ACTIVE, [active_event_key, raw_event])
            end
            unpack(raw_event)
          end
        end
      end

      SELECT_ACTIVE_EVENT = <<~SQL
        SELECT payload FROM dp_active_events WHERE key = $1;
      SQL
      def processing_event
        with_postgres do |c|
          raw_event = PG.single(
            c.exec(SELECT_ACTIVE_EVENT, [active_event_key])
          )
          unpack(raw_event)
        end
      end

      private

      def pack(event)
        JSON.dump(
          "evid" => event.id,
          "txid" => event.tx_id,
          "transit" => event.transition,
          "args" => event.args,
          "source" => event.source,
          "destination" => event.destination
        )
      end

      def unpack(packed_event)
        return unless packed_event
        unpacked_event = JSON.load(packed_event)
        Event.new(
          data: {
            "uuid" => unpacked_event["evid"],
            "transaction_uuid" => unpacked_event["txid"],
            "transition" => unpacked_event["transit"],
            "args" => unpacked_event["args"],
            "source" => unpacked_event["source"],
            "destination" => unpacked_event["destination"]
          }
        )
      end

      def events_queue_key
        "#{@root}:events"
      end

      def active_event_key
        "#{@root}:active"
      end
    end
  end
end
