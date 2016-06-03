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
    @logs << LogEntry.new(term, Misc::PREPARE, message, @peers.length/2 + 1)
    flush
  end

  # @param: to_index: Commit local log till index
  def commit_log_till_index(to_index)
    @logs.each_with_index do |log, index|
      if(index <= to_index)
        log.type = Misc::COMMITTED
      else
        break
      end
    end
  end

  # @description: Add entry at specific index
  # and clear all entries after this index
  def add_entry_at_index(entry, index)
    if index > @logs.length
      raise 'Exception while adding log: Too large index'
    elsif index == @logs.length
      @logs << entry
    else
      while @logs.length != index
        @logs.pop
      end
      @logs << entry
    end
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
    attr_accessor(:term, :type, :message, :ack_count, :quorum)

    def initialize(term, type, message, quorum)
      @term = term
      @type = type
      @message = message
      # Quorum num for this entry
      @quorum = quorum
      # Creator must ack it
      @ack_count = 1
    end



    def to_s
      "#{@term}\t#{@type}\t#{@message}\n"
    end
  end


end