module DirtyPipeline
  module Redis
    class Railway
      DEFAULT_OPERATIONS = %w(call undo finalize)

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

      def clear!
        @queues.values.each(&:clear!)
        DirtyPipeline.with_redis do |r|
          r.multi do |mr|
            mr.del(active_operation_key)
            mr.del(active_transaction_key)
          end
        end
      end

      def next
        return if other_transaction_in_progress?
        start_transaction! unless running_transaction

        queue.pop.tap { |task| finish_transaction! if task.nil? }
      end

      def queue(operation_name = active)
        @queues.fetch(operation_name.to_s) do
          @queues.store(operation_name, create_queue(operation_name))
        end
      end
      alias :[] :queue

      def switch_to(name)
        raise ArgumentError unless DEFAULT_OPERATIONS.include?(name.to_s)
        return if name.to_s == active
        DirtyPipeline.with_redis { |r| r.set(active_operation_key, name) }
      end

      def active
        DirtyPipeline.with_redis { |r| r.get(active_operation_key) }
      end
      alias :operation :active

      def running_transaction
        DirtyPipeline.with_redis { |r| r.get(active_transaction_key) }
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

      def start_transaction!
        switch_to(DEFAULT_OPERATIONS.first) unless active
        DirtyPipeline.with_redis { |r| r.set(active_transaction_key, @tx_id) }
      end

      def finish_transaction!
        clear! if running_transaction == @tx_id
      end
    end
  end
end
