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

  Thread.abort_on_exception=true # add this for handling non-thread Thread exception

  attr_accessor(:name,
                :leader,
                :conn,
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

    @leader = nil
    @name = name

    #Persistent State
    @current_term = 1
    @voted_for = nil

    # Volatile State, map: name=>Peer
    @peers = {}


    @logs = []

    #Volatile State
    @commit_index = 0
    @last_applied = 0



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
    @current_state.threads.each do |thread|
      Thread.kill(thread)
    end
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

  # Term switching
  def new_term
    @current_term += 1
    reset
  end

  def change_term(term)
    @current_term = term
    reset
  end

  # @description: triggered when there is a new term
  def reset
    @voted_for = nil
    @leader = nil
    @peers.values.each do |peer|
      peer.reset
    end
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
      ind = add_log_entry(payload)
      @logger.info "#{all_log_to_string}"
      while true
        if @logs[ind].type == Misc::COMMITTED
          break
        end
        sleep(Misc::STATE_LOOP_INTERVAL)
      end
      @client_post_direct_exchange.publish('Successfully posted',
                                           :routing_key => properties.reply_to,
                                           :correlation_id => properties.correlation_id)
    # if I am not the leader, I'll forward the message to leader
    elsif @current_state.is_a?(Follower)
      if @leader.nil?
        @client_post_direct_exchange.publish('Failed, no leader',
                                             :routing_key => properties.reply_to,
                                             :correlation_id => properties.correlation_id)
      else
        @client_post_direct_exchange.publish(payload,
                                            :routing_key => "#{@leader}_client_post_queue",
                                             :reply_to => properties.reply_to,
                                            :correlation_id => properties.correlation_id)
      end

    end
  end

  def respond_to_lookup(delivery_info, properties, payload)
    # send my committed logs to client
    @client_lookup_direct_exchange.publish(committed_log_to_string,
                                           :routing_key => properties.reply_to,
                                           :correlation_id => properties.correlation_id)

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







#
# dc2 = DataCenter.new('dc2','169.231.10.109', true)
# t2 = Thread.new do
#   dc2.run
# end
#
#
# dc2.add_log_entry('Message1')
# dc2.add_log_entry('Message2')
# dc2.add_log_entry('Message3')
# dc2.add_log_entry('Message4')
# #
# #
# # sleep(8)
# # # sleep(1)
# # #
# dc3= DataCenter.new('dc3','169.231.10.109')
# t3 = Thread.new do
#   dc3.run
# end
#
#
# dc1 = DataCenter.new('dc1','169.231.10.109')
# t1 = Thread.new do
#   dc1.run
# end
#
#
# t2.join
# t3.join
# t1.join

# dc3 = DataCenter.new('dc3','169.231.10.109')
# t3 = Thread.new do
#   dc3.run
# end

# # t1.join
# t2.join
# t3.join

# t3.join