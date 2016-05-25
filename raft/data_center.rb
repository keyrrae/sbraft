require 'bunny'
require_relative 'log'
require_relative 'misc'

class DataCenter

  attr_accessor(:datacenter_name, :quorum, :log, :current_term, :fsm, :state, :timer)

  def initialize(datacenter_name, ip)
    self.datacenter_name = datacenter_name
    @cfg_filename = 'configuration.txt'
    @peers = {}

    begin
      file = File.open(@cfg_filename,'r')
      puts "Found configuration file #{@cfg_filename}"

      file.readlines.each do |line|
        dc_name = line.strip
        if dc_name != self.datacenter_name
          @peers[dc_name] = Peer.new(dc_name)
        end

      end

    rescue
      return false
    end


    self.log = Log.new(datacenter_name)


    self.current_term = 1
    @conn = Bunny.new(:hostname => ip)
    @conn.start

    @ch = @conn.create_channel
    @msg_queue = @ch.queue('hello')

    @voted_for = nil
    @commit_index = 0


  end

  def new_term
    self.current_term += 1
  end

  def run
    puts 'start'
    t1 = Thread.new do
      while true

      end
    end
    t1.join
  end
=begin
  def run
    #Process messages(from Client and other Data Centers)
    t1 = Thread.new do
      while true
        @msg_queue.subscribe(:block => true) do |delivery_info, properties, body|
          puts " Received #{body}"

          case body
            when 'hello'
              @signal_queue << 'hello'
            when 'no'
              @signal_queue << 'stop'
            else

          end

          # cancel the consumer to exit
          delivery_info.consumer.cancel
        end
      end
    end

    # Finite State Machine
    t2 = Thread.new do

      while true
        # check if timeout handle timeout

        if timer.timeout?
          puts 'timeout'
          handle_timeout
        end

        if @signal_queue.empty?
          next
        else
          signal = @signal_queue.shift # dequeue from front
          dispatch_normal(signal)
          puts "FSM #{signal}"
        end #if
      end # while
    end # thread

    t1.join
    t2.join
  end


  def dispatch_normal(signal)
    puts signal
    case self.role
      when :follower

      when :candidate

      when :leader
      else
    end
  end

  def handle_timeout

    case self.role
      when :follower
        @current_term += 1
        self.role = :candidate
        puts 'send requestVote'
        # TODO: send requestVote
      when :candidate
        @current_term += 1
        puts 'send requestVote'
        # TODO: send requestVote
      else
    end

  end

=end
end


class VoteTimer < Timer

  attr_accessor(:last_timestamp, :timeout_milli)

  def initialize
    self.timeout_milli = rand(start..stop)
    self.last_timestamp = Time.now
  end

end

class HeartbeatTimer < Timer

  attr_accessor(:last_timestamp, :timeout_milli)

  def initialize(timeout_milli)
    super
    self.timeout_milli = timeout_milli
  end

end


class State
  def initialize(datacenter_context, fsm_context)
    @datacenter = datacenter_context
    @fsm = fsm_context
    @election_timer = VoteTimer.new  # Reset Election Timer

  end

  def response_to_signal(datacenter_context, fsm_context, signal)
    raise "State is not implemented, cannot process #{signal}"
  end
end


class Follower < State


  def run
    t1 = Thread.new do
      # Wait for timeout
      while true
        if @election_timer.timeout?
          puts 'timeout'
          @fsm.set_state(Candidate.new(@datacenter, @fsm))
          break
        end
      end # while
    end
    t1.join
  end

  def respond_to_signal(signal)


    case signal.downcase
      when 'requestVote'
        # TODO : requestVote RPC
        puts 'send requestVote reply'

      when 'heartBeat'
        # TODO : append_entries RPC
        puts 'append_entries RPC'
        @timer.reset_timer

      else

    end # case

  end
end


class Candidate < State
  def initialize(datacenter_context, fsm_context)
    super(datacenter_context, fsm_context)
    @heart_timer = Timer.new(50)
    @datacenter.new_term  # Increment Datacenter Term
    @vote_count = 1 # Vote for self
    # TODO: send requestVote RPC to peers
    run
  end

  def run

    t1 = Thread.new do
      while true
        if @election_timer.timeout?
          @fsm.set_state(Candidate.new(@datacenter, @fsm))
          break
        end
      end

    end

    t2 = Thread.new do
      while true
        if @heart_timer.timeout?
          # TODO: send requestVote RPC to peers
        end
      end
    end

    t1.join
    t2.join
  end


  def respond_to_signal(signal)

    case signal.downcase
      when 'voted for you'
        @vote_count += 1
        if @vote_count >= @datacenter.quorum
          @fsm.set_state(Leader.new(@datacenter, @fsm))
        end
      when 'append entries'

        #if signal.term > @datacenter.term
        #  @datacenter.current_term = signal.term
        #  @fsm.current_state.set_state(Follower.new(@datacenter, @fsm))
        #end

      else
        # do nothing
    end

  end
end


class Leader < State

  def initialize(datacenter_context, fsm_context)
    super(datacenter_context, fsm_context)
    @heart_timer = Timer.new(50)
    # TODO: send initial empty AppendEntries RPCs
  end


  def run

    t1 = Thread.new do
      while true
        if @election_timer.timeout?
          @fsm.set_state(Candidate.new(@datacenter, @fsm))
          break
        end
      end

    end

    t2 = Thread.new do
      while true
        if @heart_timer.timeout?
          # TODO: send requestVote RPC to peers
        end
      end
    end

    t1.join
    t2.join
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


  def respond_to_signal(signal)
    case signal
      when 'command from client'
        @datacenter.log.add_entry(@datacenter.current_term, type, message)
        when ''

    end
  end
end


class StateContext

  attr_accessor(:current_state)
  def initialize(datacenter_context)
    self.current_state = Follower.new(datacenter_context, self)
  end

  def set_state(new_state)
    self.current_state = new_state
  end

  def respond_to_signal(signal)
    self.current_state.respond_to_signal(signal)
  end

end


class Peer
  attr_accessor(:name)

  def initialize(name)
    self.name = name

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

dc1 = DataCenter.new('dc1', '169.231.10.109')
dc1.run

dc2 = DataCenter.new('dc2', '169.231.10.109')
dc2.run