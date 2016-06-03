require_relative '../misc'
require 'pstore'

module LogContainer
  # Log's index starts at 1
  def last_log_term
    @logs.last.term
  end

  def last_log_index
    @logs.length - 1
  end

  # @description: Get last committed log's index
  def commit_index
    @logs.each_with_index do |log, index|
      next if log.nil?
      return index if log.type == Misc::COMMITTED
    end
    return 0
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
      @term = term
      @type = type
      @message = message
    end

    def committed?
      @type == Misc::COMMITTED
    end

    def to_s
      "#{@term}\t#{@type}\t#{@message}\n"
    end

  end
end