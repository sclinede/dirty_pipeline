module DirtyPipeline
  class Railway
    OPERATIONS = %w(call undo finalize)

    def initialize(subject, transaction_id)
      @tx_id = transaction_id
      @root = "dirty-pipeline-rail:#{subject.class}:#{subject.id}:" \
              ":txid_#{transaction_id}"
      @queues = Hash[
        OPERATIONS.map do |operation|
          [operation, Queue.new(operation, subject, transaction_id)]
        end
      ]
    end

    def clear!
      @queues.values.each(&:clear!)
      DirtyPipeline.with_redis { |r| r.del(active_operation_key) }
    end

    def next
      return if other_transaction_in_progress?
      start_transaction! if running_transaction.nil?

      queue.pop.tap { |event| finish_transaction! if event.nil? }
    end

    def queue(name = active)
      @queues[name.to_s]
    end
    alias :[] :queue

    def switch_to(name)
      raise ArgumentError unless OPERATIONS.include?(name.to_s)
      return if name.to_s == active
      DirtyPipeline.with_redis { |r| r.set(active_operation_key, name) }
    end

    def active
      DirtyPipeline.with_redis { |r| r.get(active_operation_key) }
    end
    alias :operation :active

    private

    def active_transaction_key
      "#{@root}:active_transaction"
    end

    def active_operation_key
      "#{@root}:active_operation"
    end

    def start_transaction!
      switch_to(OPERATIONS.first)
      DirtyPipeline.with_redis { |r| r.set(active_transaction_key, @tx_id) }
    end

    def finish_transaction!
      return unless running_transaction == @tx_id
      DirtyPipeline.with_redis { |r| r.del(active_transaction_key) }
      @queues.values.each(&:clear!)
    end

   def running_transaction
     DirtyPipeline.with_redis { |r| r.get(active_transaction_key) }
   end

    def other_transaction_in_progress?
      return false if running_transaction.nil?
      running_transaction != @tx_id
    end
  end
end
