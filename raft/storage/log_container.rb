require_relative '../misc'
require 'pstore'

module LogContainer

  def last_term
    return 0 if @logs.length == 0
    @logs.last.term
  end

  def last_index
    @logs.length
  end


  def add_log_entry(term, message)
    @logs << LogEntry.new(term, Misc::PREPARE, message)
    flush
  end

  def commit_log_entry(index)
    @logs[index].type = Misc::COMMITTED
    flush
  end


  def print_log
    index = 1
    @logs.each do |log_entry|
      puts "#{index} #{log_entry}"
      index = index + 1
    end
  end

  def flush
    @store = PStore.new("#{@name}.pstore")
    @store.transaction do
      @store[:logs] = @logs
    end
  end



  class LogEntry
    attr_accessor(:term, :type, :message)

    def initialize(term, type, message)

      self.term = term
      self.type = type
      self.message = message

    end

    def committed?
      self.type == Misc::COMMITTED
    end

    def to_s
      "#{self.term} #{self.type} #{self.message}\n"
    end

  end
end