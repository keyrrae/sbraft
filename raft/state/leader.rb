require_relative './state_module'

class Leader < State
  Thread.abort_on_exception=true # add this for handling non-thread Thread exception

  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "[#{datetime}] #{@datacenter.name}(Leader): #{msg}\n\n"
    end
  end

  # @description: Leader start
  # 1. Set each peer's next_index to @logs.length (first empty slot index in local logs， i.e last log index + 1)
  # Set each peer's match_index to 0
  # 2. Start thread for each peer, periodically listen to heartbeat_timer
  # and send out appendEntries for each timeout, and handle the reply
  def run
    @logger.info 'Leader state start'

    #Step 1
    @datacenter.peers.values.each do |peer|
      peer.next_index = @datacenter.logs.length
      peer.match_index = 0
    end

    # Step 2
    threads = []
    @datacenter.peers.values.each do |peer|
      threads << Thread.new do
        loop do
          #Break out the loop and state come to end if state got killed
          if @status == Misc::KILLED_STATE
            Thread.stop
          end

          #If HeartBeatTimer timeout, send another wave of AppendEntries
          if peer.heartbeat_timer.timeout?
            peer.heartbeat_timer.reset_timer
            begin
              append_entries_reply = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                append_entries_reply = rpc_appendEntries(peer)
              end
              @logger.info "Peer #{peer.name} respond to appendEntries rpc with: #{append_entries_reply}"
              handle_appendEntries_reply append_entries_reply
            rescue Timeout::Error
              @logger.info "Peer #{peer.name} cannot be reached by appendEntries rpc"
            end
          else
            sleep(Misc::STATE_LOOP_INTERVAL)
          end
        end
      end
    end
    threads.each do |thread|
      thread.join
    end
    @logger.info 'Leader state end'
  end


  # AppendEntries RPC
  def rpc_appendEntries(peer)
    call_id = Misc::generate_uuid
    append_entries_message = {}
    append_entries_message['term'] = @datacenter.current_term
    # Consistency check
    append_entries_message['prev_index'] = peer.next_index - 1
    append_entries_message['prev_term'] = @datacenter.logs[peer.next_index - 1].term

    # If there is only 1 difference between peer's next_index and match_index
    # If next_index have entry (e.g, When leader just received on post), send it together
    # Else just send empty entries
    append_entries_message['entries'] = nil

    if((peer.next_index - peer.match_index) == 1)
      append_entries_message['entries'] = get_entry_at(peer.next_index)
    end
    append_entries_message['commit_index'] = commit_index

    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    reply_queue.bind(@datacenter.append_entries_direct_exchange, :routing_key => reply_queue.name)

    @datacenter.append_entries_direct_exchange.publish(append_entries_message.to_json,
                                            :expiration => Misc::RPC_TIMEOUT,
                                            :routing_key => peer.append_entries_queue_name,
                                            :correlation_id => call_id,
                                            :reply_to=>reply_queue.name)
    #Wait for response
    response_result = nil
    responded = false
    while true
      reply_queue.subscribe do |delivery_info, properties, payload|
        if properties[:correlation_id] == call_id
          response_result = payload
          responded = true
        end
      end
      break if responded
    end
    response_result
  end


  # @param payload [term, prev_index, prev_term, entries, commit_index]
  # @description: Should be an error case
  def respond_to_append_entries(delivery_info, properties, payload)
    raise 'Error! Double leader condition.'
  end


  # @param payload [term, candidate_name, last_log_index, last_log_term]
  # @description: If discover greater term, change term and step down. Will not handle this message.
  # If received lower or equal term peer's vote request, reply false
  # @sent_message request_vote_reply[:term, :granted, :from]
  def respond_to_vote_request(delivery_info, properties, payload)
    payload = JSON.parse(payload)
    if payload['term'] > @datacenter.current_term
      @datacenter.change_term payload['term']
      @datacenter.change_state (Follower.new(@datacenter))
    else
      request_vote_reply = {}
      request_vote_reply['term'] = @datacenter.current_term
      request_vote_reply['granted'] = false
      request_vote_reply['from'] = @datacenter.name
      @datacenter.request_vote_direct_exchange.publish(request_vote_reply.to_json,
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)
    end
  end




  # @param append_entries_reply[term, success, match_index, from]
  # @description: Leader handle appendEntries reply from peer
  # 1. Change match_index if success. Add one peer's ack for entry
  # at match_index if there is one at that pos (Will auto commit if needed).
  # Then forward next_index pointer if there is more logs to be sent
  # 2. Decrement next_index if failed
  def handle_appendEntries_reply(append_entries_reply)
    append_entries_reply = JSON.parse(append_entries_reply)

    peer = @datacenter.peers[append_entries_reply['from']]

    #分两种情况，一种是之前的matchIndex和现在的不一样，这种情况只把matchIndex向前提到nextIndex的前一个位置，下一轮会发nextIndex位置的元素
    #第二种情况是之前的matchIndex和现在的一样，这种情况我们可以知道之前一轮的matchIndex和nextIndex只差一个，应该是把nextIndex的元素发出去了
    #这种情况双指针向前进
    if append_entries_reply['success']
      match_index = append_entries_reply['match_index']
      #情况1
      if peer.match_index != append_entries_reply['match_index']
        peer.match_index = match_index
        #情况2
      else
        @datacenter.peer_ack(peer.next_index, peer.name)
        if peer.next_index < @datacenter.logs.length
          peer.next_index = peer.next_index + 1
          peer.match_index = peer.match_index + 1
        end
      end


    else
      peer.next_index = peer.next_index - 1
    end
  end




  #
  #Upon election: send initial empty AppendEntries RPCs
  #(heartbeat) to each server; repeat during idle periods to
  #prevent election timeouts (§5.2)
  #• If command received from client: append entry to local log,
  #                                                         respond after entry applied to state machine (§5.3)
  #• If last log index ≥ nextIndex for a follower: send
  #AppendEntries RPC with log entries starting at nextIndex
  #• If successful: update nextIndex and matchIndex for
  #                                                follower (§5.3)
  #  • If AppendEntries fails because of log inconsistency:
  #                                              decrement nextIndex and retry (§5.3)
  #  • If there exists an N such that N > commitIndex, a majority
  #  of matchIndex[i] ≥ N, and log[N].term == currentTerm:
  #      set commitIndex = N (§5.3, §5.4).


end
