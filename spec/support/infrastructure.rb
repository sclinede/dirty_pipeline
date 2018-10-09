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


MAIL_ATTRIBUTES = %i(id title from to body events_store)
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

  def save!(data = to_h)
    self.id ||= DB[:mails].keys.max.to_i + 1
    DB[:mails].store(id, to_h)
  end
  alias :save :save!
end

class MailPipeline < DirtyPipeline::Base
  self.pipeline_storage = :events_store

  def self.receive(mail)
    throw :success, {"received_at" => Time.now.utc.iso8601}
  end

  def self.open(mail)
    throw :success, {"read_at" => Time.now.utc.iso8601}
  end

  def self.unread(mail)
    throw :success, {"read_at" => nil}
  end

  def self.delete(mail)
    throw :success, {"deleted_at" => Time.now.utc.iso8601}
  end

  transition :receive, from: nil,           to: :new
  transition :open,    from: :new,          to: :read
  transition :unread,  from: :read,         to: :new
  transition :delete,  from: [:read, :new], to: :deleted
end
