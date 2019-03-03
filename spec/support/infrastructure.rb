require 'time'

module DB
  module_function
  def current
    Thread.current[:test_store] ||= Hash.new
  end

  def [](table_name)
    current.fetch(table_name) { current.store(table_name, Hash.new) }
  end

  def reset!
    Thread.current[:test_store] = nil
  end
end

module ActiveRecord
  class Rollback < StandardError; end
end

MAIL_ATTRIBUTES = %i(id title from to body tasks_store)
Mail = Struct.new(*MAIL_ATTRIBUTES)
class Mail
  def self.find(id)
    new(DB[:mails].fetch(id).values_at(*MAIL_ATTRIBUTES))
  end

  def transaction(*, **)
    previous_data = to_h
    yield
    save
  rescue ActiveRecord::Rollback
    save!(previous_data)
  end

  def pipeline
    @pipeline ||= MailPipeline.new(self)
  end

  def save!(data = to_h)
    self.id ||= DB[:mails].keys.max.to_i + 1
    DB[:mails].store(id, to_h)
  end
  alias :save :save!
end

class MailPipeline < DirtyPipeline::Base
  self.pipeline_storage = :tasks_store
  class << self
    def mutex
      Thread.current[:mail_mutex] ||= Mutex.new
    end
  end

  def with_subject_lock
    self.class.mutex.synchronize { yield }
  end

  class Receive < DirtyPipeline::Transition
    def call(mail)
      Failure("Too big to store") if mail.body.to_s.size > 1024
      Success("received_at" => Time.now.utc.iso8601)
    end

    def undo(mail)
      Success("received_at" => nil)
    end
  end

  class Open < DirtyPipeline::Transition
    def call(mail)
      Failure("TLDR") if mail.body.to_s.size > 512
      Success("read_at" => Time.now.utc.iso8601)
    end

    def undo(mail)
      Success("read_at" => nil)
    end
  end

  def self.Unread(*)
    throw :success, {"read_at" => nil}
  end

  def self.Delete(*)
    throw :success, {"deleted_at" => Time.now.utc.iso8601}
  end

  transition :Receive, from: [nil],           to: :new
  transition :Open,    from: :new,          to: :read
  transition :Unread,  from: :read,         to: :new
  transition :Delete,  from: [:read, :new], to: :deleted
end
