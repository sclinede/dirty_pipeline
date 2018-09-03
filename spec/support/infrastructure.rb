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

MAIL_ATTRIBUTES = %i(id title from to body events_store)
class Mail < Struct.new(*MAIL_ATTRIBUTES)
  def self.find(id)
    new(DB[:mails].fetch(id).values_at(*MAIL_ATTRIBUTES))
  end

  def transaction
    yield
    save
  rescue ActiveRecord::Rollback
    nil
  end

  def save!
    self.id ||= DB[:mails].keys.max.to_i + 1
    DB[:mails].store(id, to_h)
  end
  alias :save :save!
end
