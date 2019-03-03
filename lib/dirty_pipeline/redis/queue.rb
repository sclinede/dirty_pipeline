module DirtyPipeline
  module Redis
    class Queue
      def initialize(operation, subject_class, subject_id, transaction_id)
        @root = "dirty-pipeline-queue:#{subject_class}:#{subject_id}:" \
                "op_#{operation}:txid_#{transaction_id}"
      end

      def clear!
        DirtyPipeline.with_redis do |r|
          r.del active_task_key
          r.del tasks_queue_key
        end
      end

      def to_a
        DirtyPipeline.with_redis do |r|
          r.lrange(tasks_queue_key, 0, -1).map! do |packed_task|
            Task.unpack(packed_task)
          end
        end
      end

      def push(task)
        DirtyPipeline.with_redis { |r| r.rpush(tasks_queue_key, task.to_json) }
        self
      end
      alias :<< :push

      def unshift(task)
        DirtyPipeline.with_redis { |r| r.lpush(tasks_queue_key, task.to_json) }
        self
      end

      def pop
        DirtyPipeline.with_redis do |r|
          data = r.lpop(tasks_queue_key)
          data.nil? ? r.del(active_task_key) : r.set(active_task_key, data)
          Task.unpack(data)
        end
      end

      def processing_task
        DirtyPipeline.with_redis { |r| Task.unpack(r.get(active_task_key)) }
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
