module DirtyPipeline
  class Base
    DEFAULT_RETRY_DELAY = 5 * 60 # 5 minutes
    DEFAULT_CLEANUP_DELAY = 60 * 60 * 24 # 1 day

    class InvalidTransition < StandardError; end

    class << self
      def find_subject(*args)
        fail NotImplemented
      end

      attr_reader :transitions_map
      def inherited(child)
        child.instance_variable_set(
          :@transitions_map,
          transitions_map || Hash.new
        )
      end
      attr_accessor :pipeline_storage, :retry_delay, :cleanup_delay

      using StringCamelcase

      def transition(name, from:, to:, action: nil, attempts: 1)
        action ||= method(name) if respond_to?(name)
        action ||= const_get(name.to_s.camelcase(:upper))
        @transitions_map[name.to_s] = {
          action: action,
          from: Array(from).map(&:to_s),
          to: to.to_s,
          attempts: attempts,
        }
      end
    end

    attr_reader :subject, :storage, :status, :uuid, :queue, :railway
    def initialize(subject, uuid: nil)
      @uuid = uuid || SecureRandom.uuid
      @subject = subject
      @storage = Storage.new(subject, self.class.pipeline_storage)
      @railway = Railway.new(subject, @uuid)
      @status = Status.success(subject)
    end

    def find_transition(name)
      self.class.transitions_map.fetch(name.to_s).tap do |from:, **kwargs|
        next unless railway.operation.eql?(:call)
        next if from == Array(storage.status)
        next if from.include?(storage.status.to_s)
        raise InvalidTransition, "from `#{storage.status}` by `#{name}`"
      end
    end

    def reset!
      railway.clear!
    end

    def clear!
      storage.reset!
      reset!
    end

    # FIXME operation :call - argument
    def chain(*args, **kwargs)
      operation = kwargs.fetch(:operation) { :call }
      railway[operation] << Event.create(*args, **kwargs.merge(tx_id: @uuid))
      self
    end

    def call
      # HANDLE ANOTHER ACTION IN PROGRESS EXPLICITLY
      return self if (enqueued_event = railway.next).nil?
      execute(load_event(enqueued_event))
    end
    alias :call_next :call

    def clean
      finished = railway.queue.to_a.empty?
      finished &&= railway.queue.processing_event.nil?
      return self if finished
      railway.switch_to(:undo)
      call
    end

    def retry
      return self if (enqueued_event = railway.queue.processing_event).nil?
      execute(load_event(enqueued_event), attempt_retry: true)
    end

    def schedule(operation = "call", delay = nil)
      job_args = {
        "transaction_id" => @uuid,
        "enqueued_pipeline" => self.class.to_s,
        "find_subject_args" => find_subject_args,
        "operation" => operation,
      }

      if delay.nil?
        ::DirtyPipeline::Worker.perform_async(job_args)
      else
        ::DirtyPipeline::Worker.perform_in(delay, job_args)
      end
    end

    def cleanup_delay; self.class.cleanup_delay || DEFAULT_CLEANUP_DELAY; end
    def schedule_cleanup; schedule("cleanup", cleanup_delay); end

    def retry_delay; self.class.retry_delay || DEFAULT_RETRY_DELAY; end
    def schedule_retry; schedule("retry",   retry_delay); end

    def when_skipped
      yield(nil, self) if railway.other_transaction_in_progress?
      self
    end

    def when_success
      yield(status.data, self) if status.success?
      self
    end

    def when_failure(tag = status.tag)
      yield(status.data, self) if status.failure? && status.tag == tag
      self
    end

    private

    def execute(event, attempt_retry: false)
      attempt_retry ? event.attempt_retry! : event.start!

      # dispatch event?
      Transaction.new(self, event).call do |destination, action, *args|
        state_changes = process_action(action, event, *args)
        next if status.failure?
        Success(event, state_changes, destination)
      end
      call_next

      self
    end

    def load_event(enqueued_event)
      storage.find_event(enqueued_event.id) || enqueued_event
    end

    def process_action(action, event, *args)
      catch(:success) do
        return if interupt_on_error(event) do
          throw :success, run_operation(action, event, *args)
        end
        nil
      end
    rescue => exception
      Failure(event, exception, type: :exception)
      raise
    end

    def run_operation(action, event, *args)
      raise ArgumentError unless action
      return unless action.respond_to?(operation = railway.active)
      action.public_send(operation, event, self, *args)
    end

    def interupt_on_error(event)
      return unless (fail_cause = catch(:fail_transition) { yield; nil })
      Failure(event, fail_cause, type: :error)
      throw :abort_transaction, true
    end

    def find_subject_args
      subject.id
    end

    def Failure(event, cause, type:)
      event.failure!
      @status = Status.failure(cause, tag: type)
    end

    def Success(event, changes, destination)
      event.complete(changes, destination)
      @status = Status.success(subject)
    end
  end
end
