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
      if log.nil?
        next
      end
      if log.type == Misc::COMMITTED
        next
      end
      return index - 1
    end
    return @logs.length - 1
  end

  # @description: Get entry at specific index
  # @return: LogEntry, or nil if index is out of range
  def get_entry_at(index)
    return nil if index >= @logs.length
    return @logs[index]
  end

  # @description: Invoked only by Leader. Accept client's request and
  # append one log entry to Logs, with self-ack this log entry.
  def add_log_entry(message)
    entry =  LogEntry.new(@current_term, Misc::PREPARE, message, @peers.length/2 + 1)
    @logs << entry
    # Self-ack
    peer_ack(@logs.length - 1,@name)
    flush
    @logs.length - 1
  end



  # @description: Invoked only by leader. An entry got a peer's ack. Will commit if reach quorum.
  # Can be triggered on a empty slot. Will just ignore this operation.
  # TODO: Not tested
  def peer_ack(entry_index, peer_name)
    if entry_index < @logs.length
      @logs[entry_index].ack_peers.add(peer_name)
    else
      return
    end

    # Mark log entries committed if stored on a majority of
    # servers and at least one entry from current term is stored on
    # a majority of servers
    # TODO: Not tested yet
    return if !majority_ack_current_term_entry?

    # If there is a majority of servers that ack a current term entry, go through all previous log
    # to see if previous logs need to be committed
    (1..entry_index).each do |index|
      if @logs[index].ack_peers.size >= @logs[index].quorum
        if @logs[index].type != Misc::COMMITTED
          @logger.info "Leader is committing #{@logs[index]}"
          @logs[index].type = Misc::COMMITTED
        end
      end
    end

    flush
  end

  # @tested
  # @description: Invoked only by leader. Check if current logs contains a current term entry that acknowledged by majority of servers
  def majority_ack_current_term_entry?
    current_term_entry = nil
    @logs.each do |log|
      if log.term == @current_term
        current_term_entry = log
        break
      end
    end
    if (current_term_entry == nil or current_term_entry.ack_peers.size < current_term_entry.quorum)
      return false
    else
      return true
    end

  end

  # @description: Invoked only by non-leader. Add entry at specific index
  # and clear all entries after this index. Do not self-ack this entry.
  # TODO Not tested
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
    flush
  end

  # @param: to_index
  # @description: Invoked only by non-leader. Commit local log till to_index
  def commit_log_till_index(to_index)
    @logs.each_with_index do |log, index|
      if(index <= to_index)
        log.type = Misc::COMMITTED
      else
        break
      end
    end
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
      s = "#{s}\n#{index}\t#{log_entry}"
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
    end
  end




  # @description: LogEntry
  # 1. ack_peers is a set recording how many peers has ack this LogEntry(i.e store this entry in their local log)
  # 2. quorum is a set of DC names. When we check if this entry could be marked as COMMITTED, just check if ack_peers set
  # has more than half of DC in quorum set. (For usual case where no configuration change is happening. Config change scenario described below)
  # 3. is_special indicate whether this is a configuration change message.  When it's a configuration change message.
  class LogEntry
    attr_accessor(:term, :type, :message, :ack_peers, :quorum, :is_special)

    def initialize(term, type, message, quorum, is_special = false)
      @term = term
      @type = type
      @message = message
      # A list of DC names. When
      @quorum = quorum
      # Creator must ack it
      @ack_peers = Set.new([])
      # Is that a Configuration change log
      @is_special = is_special
    end

    def to_json(options = {})
      {'term'=> @term, 'type'=> @type, 'message'=>@message, 'quorum'=>@quorum, 'is_special'=>@is_special}.to_json
    end

    def self.from_hash (data)
      self.new data['term'],data['type'],data['message'],data['quorum'], data['is_special']
    end

    def to_s
      "#{@term}\t#{@type}\t#{@message}"
    end
  end


end