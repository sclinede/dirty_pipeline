module DirtyPipeline
  class Rollback < StandardError; end

  # use Dry::Initializer ?
  class Transition
    extend Dry::Initializer

    param :event
    param :railway

    class << self
      attr_accessor :policies
      def inherited(klass)
        klass.policies = Hash.new
      end

      def policy(policy_settings)
        policy_settings.each_pair do |policy_name, policy_klass|
          self.policies.store(policy_name, policy_klass)
        end
      end

      def define_error(key)
        [CustomError.new(self.name, key)]
      end
    end

    CustomError = Struct.new(:class_name, :message_key, :field)
    class CustomError
      def message
        I18n.t(message_key, scope: i18n_scope)
      end

      private

      def i18n_scope
        [:pipelines, class_name.underscore]
      end
    end


    def Failure(error)
      railway&.switch_to(:undo)
      throw :fail_transition, error
    end

    def Success(changes = nil)
      case railway&.active
      when "finalize_undo"
        railway&.switch_to(:undo)
      when "undo"
        railway&.switch_to(:finalize_undo) if respond_to?(:finalize_undo)
      when "call"
        railway&.switch_to(:finalize) if respond_to?(:finalize)
      when "finalize"
        railway&.switch_to(:call)
      end
      throw :success, changes.to_h
    end

    def self.finalize_undo(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      return unless instance.respond_to?(:finalize_undo)
      instance.finalize_undo(pipeline.subject)
    end

    def self.finalize(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      return unless instance.respond_to?(:finalize)
      instance.finalize(pipeline.subject)
    end

    def self.undo(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      pipeline&.railway&.send(:[], :finalize_undo)&.send(:<<, event)
      return unless instance.respond_to?(:undo)
      instance.undo(pipeline.subject)
    end

    def self.call(*args, **kwargs)
      event, pipeline, *args = args
      instance = new(event, pipeline.railway, *args, **kwargs)
      pipeline&.railway&.send(:[], :finalize)&.send(:<<, event)
      prepare_undo(pipeline, event)
      instance.call(pipeline.subject)
    end

    def self.prepare_undo(pipeline, event)
      anti_event = event.dup
      anti_event.source, anti_event.destination =
        event.destination, event.source
      pipeline&.railway&.send(:[], :undo)&.send(:unshift, anti_event)
    end

    def cache(key)
      event.cache.fetch(key) { event.cache[key] = yield }
    end

    def state(subject)
      subject.send(pipeline.pipeline_storage)["state"].to_h
    end

    def validate!(policies_list)
      policies_list.each_pair do |policy_name, args|
        policy_klass = self.policies.fetch(policy_name)
        args = [args] unless args.respond_to?(:to_ary)
        policy = policy_klass.new(*args)
        Failure(policy.errors) if policy.invalid?
      end
    end
  end
end
