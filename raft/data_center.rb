require 'bunny'
require 'pstore'
require_relative './log'
require_relative './misc'
require_relative './config'
require_relative './state/state_context'

class DataCenter
  include Config
  attr_accessor(:datacenter_name,
                :quorum,
                :log,
                :current_term,
                :state_context,
                :state,
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
    @vote_request_direct_exchange = @ch.direct(Misc::VOTE_REQUEST_DIRECT_EXCHANGE)
    @vote_request_queue = @ch.queue("#{self.datacenter_name}_vote_request_queue")
    @vote_request_queue.bind(@vote_request_direct_exchange,
                             :routing_key=> @vote_request_queue.name)

    #Last step, run state machine
    #Create state context
    @state_context = StateContext.new(self,is_leader)

  end



  def new_term
    self.current_term += 1
  end

  def run
      puts "#{@datacenter_name} start"
      #Listen to AppendEntries
      @append_entries_queue.subscribe do |delivery_info, properties, payload|
        @state_context.respond_to_append_entries  delivery_info, properties, payload
      end
      #Listen to RequestVote
      @vote_request_queue.subscribe do |delivery_info, properties, payload|
        @state_context.respond_to_vote_request delivery_info, properties, payload
      end

      loop do
        sleep(0.05)
      end

      Thread.new do
        @state_context.run
      end
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

dc3 = DataCenter.new('dc3','169.231.10.109')
t3 = Thread.new do
  dc3.run
end

t1.join
t2.join
t3.join