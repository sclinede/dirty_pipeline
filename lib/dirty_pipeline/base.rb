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
        action ||= const_get(name.to_s.camelcase) rescue nil
        action ||= method(name) if respond_to?(name)
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
      @uuid = uuid || Nanoid.generate
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

    def chain(*args)
      railway[:call] << Event.create(*args, tx_id: @uuid)
      self
    end

    def call
      return self if (serialized_event = railway.next).nil?
      execute(load_event(serialized_event), :call)
    end
    alias :call_next :call

    def clean
      railway.switch_to(:undo)
      call_next
      self
    end

    def retry
      return unless (event = load_event(railway.queue.processing_event))
      execute(event, :retry)
    end

    def schedule_cleanup
      schedule("cleanup", cleanup_delay)
    end

    def schedule_retry
      schedule("retry", retry_delay)
    end

    def schedule(operation, delay = nil)
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

    def when_success
      yield(status.data, self) if status.success?
      self
    end

    def when_failure(tag = status.tag)
      yield(status.data, self) if status.failure? && status.tag == tag
      self
    end

    private

    def execute(event, tx_method)
      transaction(event).public_send(tx_method) do |destination, action, *args|
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
      return catch(:success) do
        return if interupt_on_error(event) do
          throw :success, run_operation(action, event, *args)
        end
        nil
      end
    rescue => exception
      @status = Status.failure(exception, tag: :exception)
      raise
    end

    def run_operation(action, event, *args)
      return unless action.respond_to?(operation = railway.operation)
      action.public_send(operation, event, self, *args)
    end

    def interupt_on_error(event)
      return unless (fail_cause = catch(:fail_operation) { yield; nil })
      Failure(event, fail_cause)
    end

    def find_subject_args
      subject.id
    end

    def retry_delay
      self.class.retry_delay || DEFAULT_RETRY_DELAY
    end

    def cleanup_delay
      self.class.cleanup_delay || DEFAULT_CLEANUP_DELAY
    end

    def transaction(event)
      Transaction.new(self, event)
    end

    def Failure(event, cause)
      event.failure!
      railway.switch_to(:undo)
      if cause.eql?(:abort)
        @status = Status.failure(subject, tag: :aborted)
      else
        @status = Status.failure(cause, tag: :error)
      end
      throw :abort_transaction, true
    end

    def Success(event, changes, destination)
      event.complete(changes, destination)
      @status = Status.success(subject)
    end
  end
end
