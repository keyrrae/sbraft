require_relative './state_module'


class Follower < State
  Thread.abort_on_exception=true # add this for handling non-thread Thread exception

  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "[#{datetime}] #{@datacenter.name}(Follower): #{msg}\n\n"
    end
  end

  def run
    @logger.info 'Follower state start'
    loop do
      #Break out the loop and state come to end if state got killed
      if @status == Misc::KILLED_STATE
        @logger.info 'Exit follower state'
        Thread.kill(Thread.current)
      end

      if @election_timer.timeout?
        @election_timer.reset_timer
        @logger.info 'Follower time out. To candidate state'
        @datacenter.change_state (Candidate.new(@datacenter))
      end
      sleep(Misc::STATE_LOOP_INTERVAL)
    end


  end

  # @param payload: [:term, :prev_index, :prev_term, :entries, :commit_index]
  # entries: def LogEntry.initialize(term, type, message, current_peers_set, new_peers_set, is_special = false, phase = -1)
  # @sent_message: append_entries_reply: [:term, :success, :match_index, :from]
  # @description: Follower's Respond to appendEntries
  # 1. If leader's term is smaller than my current term, respond with [false, current_term, 0]
  #
  # 2. Update term if needed
  # 3. (1) If it's a normal log
  # Check prevIndex and matchIndex.
  # Send success = false and match_index = 0 if not match
  # Send success = true and match_index = payload['prev_index'] if match
  # Then add entry if there is any, clear all the logs after this log
  # (2) If it's a config log
  # 3. If response is success. Commit Local log to commit_index
  def respond_to_append_entries(delivery_info, properties, payload)
    payload = JSON.parse(payload)
    @election_timer.reset_timer
    @logger.info "#{payload}"

    #Update leader
    @datacenter.leader = payload['leader']

    # Step 1
    if payload['term'] < @datacenter.current_term
      append_entries_reply = {}
      append_entries_reply['success'] = false
      append_entries_reply['match_index'] = 0
      append_entries_reply['term'] = @datacenter.current_term
      @datacenter.append_entries_direct_exchange.publish(append_entries_reply.to_json,
                                                         :routing_key => properties.reply_to,
                                                         :correlation_id => properties.correlation_id)

    end

    # Step 2
    if payload['term'] > @datacenter.current_term
      @datacenter.change_term payload['term']
    end

    # Step 2
    append_entries_reply = {}

    is_special = payload['entries']!= nil and payload['entries']['is_special']

      append_entries_reply['from'] = @datacenter.name
      if ((payload['prev_index'] < @datacenter.logs.length) &&
          @datacenter.logs[payload['prev_index']].term == payload['prev_term'])
        append_entries_reply['success'] = true
        append_entries_reply['match_index'] = payload['prev_index']
        if !payload['entries'].nil?
          # If this is a special log, change peers
          if is_special
            @datacenter.current_peers_set = Set.new(JSON.parse(payload['entries']['current_peers_set']))
            @datacenter.new_peers_set = Set.new(JSON.parse(payload['entries']['new_peers_set']))
            @datacenter.create_peers
          end
          # Will clean all logs after this index
          @datacenter.add_entry_at_index(LogContainer::LogEntry.from_hash(payload['entries']), append_entries_reply['match_index'] + 1)
        end
      else
        @logger.info "Respond False because payload['prev_index']=#{payload['prev_index']}, @datacenter.logs.length=#{@datacenter.logs.length}"
        if @datacenter.logs[payload['prev_index']] != nil
          @logger.info "@datacenter.logs[payload['prev_index']].term=#{@datacenter.logs[payload['prev_index']].term},payload['prev_term'] = #{payload['prev_term']}"
        end
        append_entries_reply['success'] = false
        append_entries_reply['match_index'] = 0
      end
      #Change config



    # Step 3
    if append_entries_reply['success']
      @datacenter.commit_log_till_index payload['commit_index']
    end


    @datacenter.append_entries_direct_exchange.publish(append_entries_reply.to_json,
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end

  # @description
  # @param payload[:term,:candidate_name,:last_log_index,:last_log_term] CAUTION: Cannot use symbol, must use string as key e.g: payload['term']
  # @sent_message request_vote_reply[:term, :granted, :from]
  def respond_to_vote_request(delivery_info, properties, payload)
    payload = JSON.parse(payload)
    request_vote_reply = {}

    request_vote_reply[:granted] = grant_vote?(payload)

    #Update term and voted_for
    if request_vote_reply[:granted]
      @datacenter.change_term payload['term']
      @datacenter.voted_for = payload['candidate_name']
      @datacenter.flush
      @election_timer.reset_timer
    end

    request_vote_reply[:term] = @datacenter.current_term
    request_vote_reply[:from] = @datacenter.name
    @logger.info "Reply RequestVoteRPC from #{payload['candidate_name']} with : #{request_vote_reply[:granted]}"
    @datacenter.request_vote_direct_exchange.publish(request_vote_reply.to_json,
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)
  end





  # @param payload[:term,:candidate_name,:last_log_index,:last_log_term]
  # @description Should I grant vote?
  def grant_vote?(payload)
    if @datacenter.current_term > payload['term']
      return false

    elsif @datacenter.current_term == payload['term']
      # This term already voted
      return false if @datacenter.voted_for != nil
      # 5.4.1 Election restriction
      if @datacenter.last_log_term < payload['last_log_term']
        return true
      elsif @datacenter.last_log_term == payload['last_log_term']
        return true if @datacenter.last_log_index <= payload['last_log_index']
        return false
      else
        return false
      end

    else
      return true
    end

  end


end
