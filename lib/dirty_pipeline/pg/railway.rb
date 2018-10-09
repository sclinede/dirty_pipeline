module DirtyPipeline
  module PG
    class Railway
      DEFAULT_OPERATIONS = %w(call undo finalize)

      def self.create!(connection)
        connection.exec <<~SQL
          CREATE TABLE dp_active_operations (
            key TEXT CONSTRAINT primary_dp_active_operations_key PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );
          CREATE TABLE dp_active_transactions (
            key TEXT CONSTRAINT primary_dp_active_tx_key PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
          );
        SQL
      end

      def self.destroy!(connection)
        connection.exec <<~SQL
          DROP TABLE IF EXISTS dp_active_operations;
          DROP TABLE IF EXISTS dp_active_transactions;
        SQL
      end

      def initialize(subject, transaction_id)
        @tx_id = transaction_id
        @subject_class = subject.class.to_s
        @subject_id = subject.id.to_s
        @root = "dirty-pipeline-rail:#{subject.class}:#{subject.id}:"
        @queues = Hash[
          DEFAULT_OPERATIONS.map do |operation|
            [operation, create_queue(operation)]
          end
        ]
      end

      DELETE_OPERATION = <<~SQL
        DELETE FROM dp_active_operations WHERE key = $1;
      SQL
      DELETE_TRANSACTION = <<~SQL
        DELETE FROM dp_active_transactions WHERE key = $1;
      SQL
      def clear!
        @queues.values.each(&:clear!)
        DirtyPipeline.with_postgres do |c|
          c.transaction do
            c.exec DELETE_OPERATION, [active_operation_key]
            c.exec DELETE_TRANSACTION, [active_transaction_key]
          end
        end
      end

      def next
        return if other_transaction_in_progress?
        start_transaction! unless running_transaction

        queue.pop.tap { |event| finish_transaction! if event.nil? }
      end

      def queue(operation_name = active)
        @queues.fetch(operation_name.to_s) do
          @queues.store(operation_name, create_queue(operation_name))
        end
      end
      alias :[] :queue

      SWITCH_OPERATION = <<~SQL
        INSERT INTO dp_active_operations (key, name) VALUES ($1, $2)
        ON CONFLICT (key)
        DO UPDATE SET name = EXCLUDED.name;
      SQL
      def switch_to(name)
        raise ArgumentError unless DEFAULT_OPERATIONS.include?(name.to_s)
        return if name.to_s == active

        DirtyPipeline.with_postgres do |c|
          c.transaction do
            c.exec(SWITCH_OPERATION, [active_operation_key, name])
          end
        end
      end

      SELECT_OPERATION = <<~SQL
        SELECT name FROM dp_active_operations WHERE key = $1;
      SQL
      def active
        DirtyPipeline.with_postgres do |c|
          PG.single c.exec(SELECT_OPERATION, [active_operation_key])
        end
      end
      alias :operation :active

      SELECT_TRANSACTION = <<~SQL
        SELECT name FROM dp_active_transactions WHERE key = $1;
      SQL
      def running_transaction
        DirtyPipeline.with_postgres do |c|
          PG.single c.exec(SELECT_TRANSACTION, [active_transaction_key])
        end
      end

      def other_transaction_in_progress?
        return false if running_transaction.nil?
        running_transaction != @tx_id
      end

      private

      def create_queue(operation_name)
        Queue.new(operation_name, @subject_class, @subject_id, @tx_id)
      end

      def active_transaction_key
        "#{@root}:active_transaction"
      end

      def active_operation_key
        "#{@root}:active_operation"
      end

      SWITCH_TRANSACTION = <<~SQL
        INSERT INTO dp_active_transactions (key, name) VALUES ($1, $2)
        ON CONFLICT (key)
        DO UPDATE SET name = EXCLUDED.name;
      SQL
      def start_transaction!
        switch_to(DEFAULT_OPERATIONS.first) unless active
        DirtyPipeline.with_postgres do |c|
          c.transaction do
            c.exec(SWITCH_TRANSACTION, [active_transaction_key, @tx_id])
          end
        end
      end

      def finish_transaction!
        clear! if running_transaction == @tx_id
      end
    end
  end
end
