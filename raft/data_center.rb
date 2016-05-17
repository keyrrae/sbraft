require 'bunny'
require_relative 'log'

class DataCenter

  attr_accessor(:datacenter_name, :log, :fsm, :state, :timer)

  def initialize(datacenter_name, ip)
    self.datacenter_name = datacenter_name

    #@timeout_milli = 1000

    @current_term = 1
    @conn = Bunny.new(:hostname => ip)
    @conn.start

    @ch = @conn.create_channel
    @msg_queue = @ch.queue('hello')
    @signal_queue = []


  end

  def time_diff_milli(start, finish)
    (finish - start) * 1000.0
  end

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


end


class Timer

  attr_accessor(:last_timestamp, :timeout_milli)

  def initialize
    self.timeout_milli = rand(100..500)
    self.last_timestamp = Time.now
  end

  def timeout?
    temp = Time.now
    if time_diff_milli(@last_timestamp, temp) > @timeout_milli then
      @last_timestamp = temp
      true
    else
      false
    end
  end

  def time_diff_milli(start, finish)
    (finish - start) * 1000.0
  end
end


class State
  def response_to_signal(context, signal)
    raise "State is not implemented, cannot process #{signal}"
  end
end


class Candidate < State
  def respond_to_signal(context, signal)

  end
end


class Follower < State
  attr_accessor(:timer)


  def respond_to_signal(context, signal)
    if signal.downcase == 'start'
      self.timer = Timer.new
      while(true)
        if self.timer.timeout?
          puts 'timeout'
          context.set_state(Candidate.new)
        end
      end
    end
  end
end


class Leader < State
  def respond_to_signal(context, signal)

  end
end


class StateContext

  attr_accessor(:current_state)
  def initialize
    self.current_state = Follower.new
  end

  def set_state(new_state)
    self.current_state = new_state
  end

  def respond_to_signal(signal)
    self.current_state.respond_to_signal(self, signal)
  end

end


class FiniteStateMachine
  attr_accessor(:role, :timer)
  def initialize
    self.role = :follower
    self.timer = Timer.new
  end

end



dc1 = DataCenter.new('dc1', '169.231.10.109')
dc1.run
puts dc1.timeout.to_s

