require_relative './state_module'

# Increase current term, vote for self
# Reset election timeout
# Send RequestVote RPCs to all other servers, wait for either:
#   Votes received from majority of servers: become leader
#   AppendEntries RPC received from new leader: step down
#   Election timeout elapse without election resolution: increase term, start new election
#   Discover higher term: step down

class Candidate < State
  Thread.abort_on_exception=true # add this for handling non-thread Thread exception

  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "[#{datetime}] #{@datacenter.name}(Candidate): #{msg}\n\n"
    end
    # Will increment Datacenter term and reset peers
    @datacenter.new_term
    @datacenter.voted_for = @datacenter.name

  end

  # @description: Candidate loop
  # 1. For each peer start a thread.
  # 2. If electionTimer timeout, increase term and reset peers, voted_for itself
  # 3. Each peer thread: If already queried that peer, sleep for a interval
  # If not, invoke RequestVoteRPC and mark it as queried if responded(no matter granted or not)
  # 4. Handle peer's response by handle_request_vote_reply
  def run
    @logger.info 'Candidate state start'
    @threads = []

    @datacenter.peers.values.each do |peer|
      @threads << Thread.new do
        loop do
          if @election_timer.timeout?
            @logger.info "Term #{@datacenter.current_term} timeout. Increase term."
            @election_timer.reset_timer

            @datacenter.new_term
            @datacenter.voted_for = @datacenter.name
          end

          # Make sure won't query a peer that's already responded
          if !peer.queried
            begin
              request_vote_reply = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                request_vote_reply = rpc_requestVote(peer)
              end
              peer.queried = true
              handle_requestVote_reply request_vote_reply
            rescue Timeout::Error
              @logger.info "RequestVoteRPC to #{peer.name} timeout"
            end
          else
            sleep(Misc::STATE_LOOP_INTERVAL)
          end
        end
      end
    end
    counter = 0
    @threads.each do |thread|
      counter +=1
      @logger.info "#{counter} join"
      thread.join
    end
    @logger.info 'Candidate state end'
  end



  # RequestVote RPC
  def rpc_requestVote(peer)
    call_id = Misc::generate_uuid
    request_vote_message = {}
    request_vote_message['term'] = @datacenter.current_term
    request_vote_message['candidate_name'] = @datacenter.name
    request_vote_message['last_log_index'] = @datacenter.last_log_index
    request_vote_message['last_log_term'] =  @datacenter.last_log_term

    ch = @datacenter.conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    reply_queue.bind(@datacenter.request_vote_direct_exchange, :routing_key => reply_queue.name)

    @datacenter.request_vote_direct_exchange.publish(request_vote_message.to_json,
                                          :expiration => Misc::RPC_TIMEOUT,
                                          :routing_key => peer.request_vote_queue_name,
                                          :correlation_id => call_id,
                                          :reply_to=>reply_queue.name)
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
  # @description: Just step down and increase term if needed. Will not handle this message,
  # leave it to follower's state loop.
  # TODO: Not tested yet
  def respond_to_append_entries(delivery_info, properties, payload)
    payload = JSON.parse(payload)
    @datacenter.current_term = payload['term']
    @datacenter.change_state(Follower.new(@datacenter))
  end

  # @param payload [term, candidate_name, last_log_index, last_log_term]
  # @description: If discover greater term, change term and step down. Will not handle this message.
  # leave it to follower's state loop.
  # If received lower or equal term peer's vote request, reply false
  # @sent_message request_vote_reply[:term, :granted, :from]
  # TODO: Not tested yet
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



  # @param request_vote_reply: [term, granted, from]
  # @description: Candidate handle requestVote reply from
  # follower(or candidate/leader, which may cause step down)
  # 1. If receive higher term, change term and step down
  # 2. If reply's term comply with sent term, and reply is true,
  # add one quorum and check if can step up(enough quorum)
  def handle_requestVote_reply(request_vote_reply)
    request_vote_reply = JSON.parse(request_vote_reply)
    term = request_vote_reply['term']

    # Step 1
    if term > @datacenter.current_term
      @datacenter.change_term term
      @datacenter.change_state (Follower.new(@datacenter))
    end

    # Step 2
    if term == @datacenter.current_term && request_vote_reply['granted']
      @datacenter.peers[request_vote_reply['from']].vote_granted = true
      @logger.info "Collect one quorum from #{request_vote_reply['from']}"
      if enough_quorum?
        @logger.info 'Got enough quorum. change state to leader'
        @datacenter.change_state(Leader.new(@datacenter))
      end
    end
  end

  # @return true if get enough votes.
  def enough_quorum?
    # At least vote for itself
    count = 1
    @datacenter.peers.values.each do |peer|
      if peer.vote_granted
        count = count + 1
      end
    end
    return true if count >= (@datacenter.peers.length / 2 + 1)
    false
  end

end