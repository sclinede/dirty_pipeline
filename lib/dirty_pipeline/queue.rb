module DirtyPipeline
  class Queue
    attr_reader :root
    def initialize(operation, subject_class, subject_id, transaction_id)
      @root = "dirty-pipeline-queue:#{subject.class}:#{subject.id}:" \
              "op_#{operation}:txid_#{transaction_id}"
    end

    def clear!
      DirtyPipeline.with_redis do |r|
        r.del active_event_key
        r.del events_queue_key
      end
    end

    def to_a
      DirtyPipeline.with_redis { |r| r.lrange(events_queue_key, 0, -1) }
    end

    def push(event)
      DirtyPipeline.with_redis { |r| r.rpush(events_queue_key, pack(event)) }
    end
    alias :<< :push

    def unshift(event)
      DirtyPipeline.with_redis { |r| r.lpush(events_queue_key, pack(event)) }
    end

    def dequeue
      DirtyPipeline.with_redis do |r|
        data = r.lpop(events_queue_key)
        data.nil? ? r.del(active_event_key) : r.set(active_event_key, data)
        return unpack(data)
      end
    end
    alias :pop :dequeue

    def processing_event
      DirtyPipeline.with_redis { |r| unpack(r.get(active_event_key)) }
    end

    private

    def pack(event)
      JSON.dump(
        "evid" => event.id,
        "txid" => event.tx_id,
        "transit" => event.transition,
        "args" => event.args,
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
        }
      )
    end

    def events_queue_key
      "#{root}:events"
    end

    def active_event_key
      "#{root}:active"
    end
  end
end
