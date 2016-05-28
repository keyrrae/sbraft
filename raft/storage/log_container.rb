require_relative '../misc'
require 'pstore'

module LogContainer

  def last_log_term
    return 0 if @logs.length == 0
    @logs.last.term
  end

  def last_log_index
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

  def committed_log_to_string
    s = ''
    index = 1
    @logs.each do |log_entry|
      if log_entry.type == Misc::COMMITTED
        s = "#{s}\n#{index}\t#{log_entry.message}"
        index += 1
      end
    end
    s = "#{s}\n\n"
    s
  end

  def all_log_to_string
    s = ''
    index = 1
    @logs.each do |log_entry|
      s = "#{s}#{index}\t#{log_entry}"
      index += 1

    end
    s = "#{s}\n"
    s
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
      @store[:commit_index] = @commit_index
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
      "#{self.term}\t#{self.type}\t#{self.message}\n"
    end

  end
end