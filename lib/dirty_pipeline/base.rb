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
          :@transitions_map, transitions_map.to_h.dup
        )
      end
      attr_accessor :pipeline_storage, :retry_delay, :cleanup_delay,
                    :background_queue

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

    def could?(tname)
      return true if railway.active.to_s != "call"
      transition = self.class.transitions_map[tname]
      return false unless transition
      from = transition[:from]
      from = [from] unless from.respond_to?(:to_ary)
      from.include?(storage.status.to_s)
    end

    def find_transition!(event, tname: nil)
      tname ||= event.transition
      event.source = storage.status
      self.class.transitions_map.fetch(tname.to_s).tap do |from:, **kwargs|
        next unless railway.operation.eql?("call")
        next if from == Array(event.source)
        next if from.include?(event.source.to_s)
        raise InvalidTransition, "from `#{event.source}` by `#{tname}`"
      end.tap do |to:, **|
        event.destination = to if railway.operation.eql?("call")
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
      with_subject_lock { call_next }
      self
    end

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
        ::DirtyPipeline::Worker
          .set(queue: self.class.background_queue || :default)
          .perform_async(job_args)
      else
        ::DirtyPipeline::Worker
          .set(queue: self.class.background_queue || :default)
          .perform_in(delay, job_args)
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

    # just an interface, implement later
    def with_subject_lock
      fail NotImplementedError
    end

    def call_next
      return self if (enqueued_event = railway.next).nil?
      unless could?(enqueued_event.transition)
        return call_next if enqueued_event.try_next?
        reset!
        return self
      end
      execute(load_event(enqueued_event))
    end

    def execute(event, attempt_retry: false)
      attempt_retry ? event.attempt_retry! : event.start!

      Transaction.new(self, event).call do |action, *args|
        state_changes = process_action(action, event, *args)

        event.assign_changes(state_changes)
        event.complete if event.start?

        next if status.failure?
        Success(event)
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
      return unless (operation = railway.active)
      return unless action.respond_to?(operation)
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

    def Success(event)
      @status = Status.success(subject)
    end
  end
end
