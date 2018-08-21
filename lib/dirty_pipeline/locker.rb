module DirtyPipeline
  class Locker
    CLEAN = "clean".freeze

    attr_reader :storage, :subject
    def initialize(subject, storage)
      @subject = subject
      @storage = storage
    end

    class Normal
      attr_reader :locker, :transition, :transition_args
      def initialize(locker, args)
        @locker = locker
        @transition = args.shift
        @transition_args = args
      end

      # NORMAL MODE
      # if state is PROCESSING_STATE - finish
      # if state is FAILED_STATE - finish
      # otherwise - start
      def skip_any_action?
        [
          Storage::PROCESSING_STATUS,
          Storage::FAILED_STATUS,
        ].include?(locker.storage.pipeline_status)
      end

      def start!
        locker.storage.start!(transition, transition_args)
      end

      def lock!
        return if skip_any_action?
        start!
        begin
          yield(transition, *transition_args)
        ensure
          if locker.storage.pipeline_status == Storage::PROCESSING_STATUS
            locker.storage.reset_pipeline_status!
          end
        end
      rescue
        if locker.storage.pipeline_status == Storage::PROCESSING_STATUS
          locker.storage.commit_pipeline_status!(Storage::FAILED_STATUS)
        end
        raise
      end
    end

    class Retry < Normal
      def initialize(locker, _args)
        @locker = locker
        @transition = locker.storage.last_event["transition"]
        @transition_args = locker.storage.last_event["input"]
      end

      # RETRY MODE
      # if state is not RETRY_STATE - finish
      # if state is RETRY_STATE - start
      def skip_any_action?
        storage.status != Storage::RETRY_STATUS
      end

      def start!
        locker.storage.start_retry!
      end
    end

    # run in time much more then time to process an item
    class Clean < Retry
      ONE_DAY = 60 * 60 * 24

      def skip_any_action?
        return true if storage.status != Storage::PROCESSING_STATUS
        started_less_then_a_day_ago?
      end

      def started_less_then_a_day_ago?
        return unless (updated_at = locker.storage.last_event["updated_at"])
        updated_at > one_day_ago
      end

      def one_day_ago
        Time.now - ONE_DAY
      end
    end

    def with_lock(*args)
      lock!(*args) do |transition, *transition_args|
        yield(transition, *transition_args)
      end
    end

    def lock!(*args)
      locker_klass(*args).new(self, args).lock! { |*largs| yield(*largs) }
    end

    def locker_klass(transition, *)
      case transition
      when Storage::RETRY_STATUS
        Retry
      when CLEAN
        Clean
      else
        Normal
      end
    end
  end
end
