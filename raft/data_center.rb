require 'bunny'
require 'pstore'
require_relative './log'
require_relative './misc'
require_relative './config'
require_relative './state/state_module'

class DataCenter
  include Config
  attr_accessor(:datacenter_name,
                :quorum,
                :log,
                :current_term,
                :current_state,
                :peers,
                :store,
                :append_entries_direct_exchange,
                :vote_request_direct_exchange
  )

  def initialize(datacenter_name, ip,is_leader=false)
    @datacenter_name = datacenter_name
    @current_term = 1
    @voted_for = nil
    @log = []
    @peers = []


    #Read configuration and storage
    read_config_and_storage

    # Setup MQ
    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch = @conn.create_channel

    # Bind append_entries_queue to AppendEntriesDirect
    @append_entries_direct_exchange = @ch.direct(Misc::APPEND_ENTRIES_DIRECT_EXCHANGE)
    @append_entries_queue = @ch.queue("#{self.datacenter_name}_append_entries_queue")
    @append_entries_queue.bind(@append_entries_direct_exchange,
                               :routing_key=> @append_entries_queue.name)

    # Bind request_vote_queue to RequestVoteDirect
    @request_vote_direct_exchange = @ch.direct(Misc::REQUEST_VOTE_DIRECT_EXCHANGE)
    @request_vote_queue = @ch.queue("#{self.datacenter_name}_request_vote_queue")
    @request_vote_queue.bind(@request_vote_direct_exchange,
                             :routing_key=> @request_vote_queue.name)

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
    @current_state.stop
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
    self.current_term += 1
  end

  def run
      puts "#{@datacenter_name} start"
      #Listen to AppendEntries
      @append_entries_queue.subscribe do |delivery_info, properties, payload|
        @current_state.respond_to_append_entries  delivery_info, properties, payload
      end
      #Listen to RequestVote
      @request_vote_queue.subscribe do |delivery_info, properties, payload|
        @current_state.respond_to_vote_request delivery_info, properties, payload
      end

      #Run state machine
      start_state

  end

  # AppendEntries RPC
  def rpc_appendEntries(peer)
    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    call_id = Misc::generate_uuid
    reply_queue.bind(@append_entries_direct_exchange, :routing_key => reply_queue.name)
    @append_entries_direct_exchange.publish('Append Entries',
                                            :routing_key => peer.append_entries_queue_name,
                                            :correlation_id => call_id,
                                            :reply_to=>reply_queue.name)
    #Wait for response
    response_result = nil
    responded = false
    while true
      reply_queue.subscribe do |delivery_info, properties, payload|
        if properties[:correlation_id] == call_id
          response_result = payload.to_s
          responded = true
        end
      end
      break if responded
    end
    response_result
  end

  # RequestVote RPC
  def rpc_requestVote(peer)
    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    call_id = Misc::generate_uuid
    reply_queue.bind(@append_entries_direct_exchange, :routing_key => reply_queue.name)
    request_vote_message = {}
    request_vote_message[:term] = @current_term
    request_vote_message[:candidate_name] = @datacenter_name
    # request_vote_message[:last_log_index] = @
    @append_entries_direct_exchange.publish('Append Entries',
                                            :routing_key => peer.append_entries_queue_name,
                                            :correlation_id => call_id,
                                            :reply_to=>reply_queue.name)
    response_result = nil
    responded = false
    while true
      reply_queue.subscribe do |delivery_info, properties, payload|
        if properties[:correlation_id] == call_id
          response_result = payload.to_s
          responded = true
        end
      end
      break if responded
    end
    response_result
  end
end



class Peer
  attr_accessor(:name,
                :append_entries_queue_name,
                :next_index,
                :match_index,
                :heartbeat_timer
  )

  def initialize(name)
    @name = name
    @append_entries_queue_name = "#{self.name}_append_entries_queue"
    @vote_request_queue_name = "#{self.name}_vote_request_queue"
    @next_index = 1
    @match_index = 0
    @vote_granted = false
    #Each peer object hold a RPC_TIMEOUT_TIMER for calculating RPC timeout
    @heartbeat_timer = Misc::Timer.new(Misc::HEARTBEAT_TIMEOUT)
  end
end

dc1 = DataCenter.new('dc1','169.231.10.109',true)
t1= Thread.new do
  dc1.run
end

dc2 = DataCenter.new('dc2','169.231.10.109')
t2 = Thread.new do
  dc2.run
end

# dc3 = DataCenter.new('dc3','169.231.10.109')
# t3 = Thread.new do
#   dc3.run
# end

t1.join
t2.join

# t3.join