require 'bunny'
require 'pstore'
require 'json'
require 'logger'
require_relative './misc'
require_relative './config'
require_relative './storage/log_container'
require_relative './state/state_module'

class DataCenter
  include Config
  include LogContainer

  attr_accessor(:name,
                :current_term,
                :current_state,
                :peers,
                :store,
                :logs,
                :append_entries_direct_exchange,
                :request_vote_direct_exchange,
                :client_lookup_direct_exchange,
                :client_post_direct_exchange,
                :voted_for
  )

  def initialize(name, ip,is_leader=false)

    @logger = Logger.new($stdout)

    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{@name}: #{msg}\n"
    end


    @name = name

    #Persistent State
    @current_term = 1
    @voted_for = nil

    # Log's index start at 1
    @logs = [LogEntry.new(0,Misc::COMMITTED,'Empty')]

    #Volatile State
    @commit_index = 0
    @last_applied = 0

    # Volatile State, map: name=>Peer
    @peers = {}

    #Read configuration and storage
    read_config
    read_storage

    # Setup MQ
    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch = @conn.create_channel

    # Bind append_entries_queue to AppendEntriesDirect
    @append_entries_direct_exchange = @ch.direct(Misc::APPEND_ENTRIES_DIRECT_EXCHANGE)
    @append_entries_queue = @ch.queue("#{@name}_append_entries_queue")
    @append_entries_queue.bind(@append_entries_direct_exchange,
                               :routing_key=> @append_entries_queue.name)

    # Bind request_vote_queue to RequestVoteDirect
    @request_vote_direct_exchange = @ch.direct(Misc::REQUEST_VOTE_DIRECT_EXCHANGE)
    @request_vote_queue = @ch.queue("#{@name}_request_vote_queue")
    @request_vote_queue.bind(@request_vote_direct_exchange,
                             :routing_key=> @request_vote_queue.name)

    # Bind client_post_queue to ClientPostDirect
    @client_post_direct_exchange = @ch.direct(Misc::CLIENT_POST_DIRECT_EXCHANGE)
    @client_post_queue = @ch.queue("#{@name}_client_post_queue")
    @client_post_queue.bind(@client_post_direct_exchange,
                            :routing_key=> @client_post_queue.name)

    # Bind client_lookup_queue to ClientLookupDirect
    @client_lookup_direct_exchange = @ch.direct(Misc::CLIENT_LOOKUP_DIRECT_EXCHANGE)
    @client_lookup_queue = @ch.queue("#{@name}_client_lookup_queue")
    @client_lookup_queue.bind(@client_lookup_direct_exchange,
                              :routing_key=> @client_lookup_queue.name)

    #Last step, run state machine
    #Create state context
    if is_leader
      @current_state = Leader.new(self)
    else
      @current_state = Follower.new(self)
    end
  end

  def set_state(new_state)
    #set new state
    @current_state = new_state
  end

  def stop_state
    @current_state.status = Misc::KILLED_STATE
  end

  def start_state
    t = Thread.new do
      @current_state.run
    end
    t.join
  end

  def change_state(new_state)
    stop_state
    set_state new_state
    start_state
  end

  def new_term
    @current_term += 1
    reset
  end

  # @description: triggered when there is a new term
  def reset
    @voted_for = nil
    @peers.values.each do |peer|
      peer.reset
    end
  end

  def change_term(term)
    @current_term = term
    @voted_for = nil
  end

  def run
      @logger.info ' start'
      #Listen to AppendEntries
      @append_entries_queue.subscribe do |delivery_info, properties, payload|
        @current_state.respond_to_append_entries  delivery_info, properties, payload
      end
      #Listen to RequestVote
      @request_vote_queue.subscribe do |delivery_info, properties, payload|
        @current_state.respond_to_vote_request delivery_info, properties, payload
      end

      @client_post_queue.subscribe do |delivery_info, properties, payload|
        respond_to_post delivery_info, properties, payload
      end

      @client_lookup_queue.subscribe do |delivery_info, properties, payload|
        respond_to_lookup delivery_info, properties, payload
      end

      #Run state machine
      start_state

  end

  def respond_to_post(delivery_info, properties, payload)

    # if I am the leader, I'll add the message to my logs
    if @current_state.is_a?(Leader)
      #@logs << payload.to_s
      add_log_entry(@current_term, payload)
      @logger.info "#{all_log_to_string}"
      @client_post_direct_exchange.publish('Successfully posted',
                                           :routing_key => properties.reply_to,
                                           :correlation_id => properties.correlation_id)
    # if I am not the leader, I'll forward the message to leader
    elsif @current_state.is_a?(Follower)
      @client_post_direct_exchange.publish(payload,
                                           :routing_key => properties.reply_to,
                                           :correlation_id => properties.correlation_id)
    end
  end

  def respond_to_lookup(delivery_info, properties, payload)
    # send my committed logs to client
    @client_lookup_direct_exchange.publish(all_log_to_string,
                                           :routing_key => properties.reply_to,
                                           :correlation_id => properties.correlation_id)

  end

  # AppendEntries RPC
  def rpc_appendEntries(peer)
    call_id = Misc::generate_uuid
    append_entries_message = {}
    append_entries_message['term'] = @current_term
    # Consistency check
    append_entries_message['prev_index'] = peer.next_index - 1
    append_entries_message['prev_term'] = @logs[peer.next_index - 1].term

    # If there is only 1 difference between peer's next_index and match_index
    # If next_index have entry (e.g, When leader just received on post), send it together
    # Else just send empty entries
    append_entries_message['entries'] = nil
    if((peer.next_index - peer.match_index) == 1 && (peer.next_index < @logs.length))
        append_entries_message['entries'] = @logs[peer.next_index]
    end
    append_entries_message['commit_index'] = commit_index

    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    reply_queue.bind(@append_entries_direct_exchange, :routing_key => reply_queue.name)

    @append_entries_direct_exchange.publish(append_entries_message.to_json,
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

  # RequestVote RPC
  def rpc_requestVote(peer)
    call_id = Misc::generate_uuid
    request_vote_message = {}
    request_vote_message['term'] = @current_term
    request_vote_message['candidate_name'] = @name
    request_vote_message['last_log_index'] = last_log_index
    request_vote_message['last_log_term'] = last_log_term

    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    reply_queue.bind(@request_vote_direct_exchange, :routing_key => reply_queue.name)

    @request_vote_direct_exchange.publish(request_vote_message.to_json,
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

  # @return true if get enough votes.
  def enough_quorum?
    count = 0
    peers.values.each do |peer|
       if peer.vote_granted
         count = count + 1
       end
    end
    return true if (count + 1) > peers.values.length / 2
    false
  end
end



class Peer
  attr_accessor(:name,
                :append_entries_queue_name,
                :request_vote_queue_name,
                :next_index,
                :match_index,
                :vote_granted,
                :heartbeat_timer,
                :queried
  )

  def initialize(name)
    @name = name
    @append_entries_queue_name = "#{self.name}_append_entries_queue"
    @request_vote_queue_name = "#{self.name}_request_vote_queue"
    @next_index = 1
    @match_index = 0
    @vote_granted = false
    # @flag: indicating if I have contacted this peer or not. If contacted(in this term), no requestVoteRpc will be sent
    @queried = false
    #Each peer object hold a RPC_TIMEOUT_TIMER for calculating RPC timeout
    @heartbeat_timer = Misc::Timer.new(Misc::HEARTBEAT_TIMEOUT)
  end

  # @description: Triggered when datacenter reach a new term
  def reset
    @vote_granted = false
    @queried = false
  end
end








dc2 = DataCenter.new('dc2','169.231.10.109', true)
t2 = Thread.new do
  dc2.run
end


sleep(3)
# sleep(1)
#
dc3= DataCenter.new('dc3','169.231.10.109')
t3 = Thread.new do
  dc3.run
end


# dc3 = DataCenter.new('dc3','169.231.10.109')
# t3 = Thread.new do
#   dc3.run
# end

# t1.join
t2.join
t3.join

# t3.join