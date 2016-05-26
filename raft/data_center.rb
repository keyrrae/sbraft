require 'bunny'
require_relative 'log'
require './misc'
require 'pstore'
require 'config'
require_relative 'state/state_context'

class DataCenter
  include Config
  attr_accessor(:datacenter_name,
                :quorum,
                :log,
                :current_term,
                :state_context,
                :state,
                :timer,
                :peers,
                :store)

  def initialize(datacenter_name, ip)
    @datacenter_name = datacenter_name
    @current_term = 1
    @voted_for = nil
    @log = []
    @peers = []
    @state_context = StateContext.new(self)


    #Read configuration and storage
    read_config_and_storage

    # Setup MQ
    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch = @conn.create_channel

    # Bind append_entries_queue to AppendEntriesDirect
    @append_entries_direct_exchange = @ch.direct('AppendEntriesDirect')
    @append_entries_queue = @ch.queue("#{self.datacenter_name}_append_entries_queue")
    @append_entries_queue.bind(@append_entries_direct_exchange,
                               :routing_key=> @append_entries_queue.name)

    # Bind request_vote_queue to RequestVoteDirect
    @request_vote_direct_exchange = @ch.direct('RequestVoteDirect')
    @request_vote_queue = @ch.queue("#{self.datacenter_name}_request_vote_queue")
    @request_vote_queue.bind(@request_vote_direct_exchange,
                             :routing_key=> @request_vote_queue.name)

    # self.log = Log.new(datacenter_name)

  end



  def new_term
    self.current_term += 1
  end

  def run
    puts "#{@datacenter_name} start"
    #Listen to AppendEntries
    @append_entries_queue.subscribe do |delivery_info, properties, payload|
      @state_context.respond_to_append_entries  [delivery_info, properties, payload]
    end
    #Listen to RequestVote
    @request_vote_queue.subscribe do |delivery_info, properties, payload|
      @state_context.respond_to_vote_request [delivery_info, properties, payload]
    end

  end

  # AppendEntries RPC
  def rpc_appendEntries(peer)
    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    call_id = Misc::generate_uuid
    reply_queue.bind(@append_entries_direct_exchange, :routing_key => reply_queue.name)
    @append_entries_direct_exchange.publish("Append Entries",
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
  attr_accessor(:name, :append_entries_queue_name)

  def initialize(name)
    @name = name
    @append_entries_queue_name = "#{self.name}_append_entries_queue"

    @next_index = 1
    @match_index = 0
    @vote_granted = false

  end
end

=begin
class FiniteStateMachine
  attr_accessor(:role, :timer)
  def initialize
    self.role = :follower
    self.timer = Timer.new
  end

end

=end
sc1 = StateContext.new(DataCenter.new('dc1','169.231.10.109'))
dc2 = DataCenter.new('dc2', '169.231.10.109')
dc2.run

dc3 = DataCenter.new('dc3', '169.231.10.109')
dc3.run

dc1 = DataCenter.new('dc1', '169.231.10.109')
dc1.run
