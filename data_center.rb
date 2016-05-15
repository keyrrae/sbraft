require 'bunny'
require_relative 'log'

class DataCenter

  attr_accessor(:datacenter_name, :log, :role, :state)

  def initialize(datacenter_name, ip)
    self.datacenter_name = datacenter_name
    self.log = Log.new(datacenter_name)
    self.role = :follower

    #@timeout_milli = 1000
    @timeout_milli = rand(100..500)

    @last_timestamp = Time.now

    @current_term = 1

    @conn = Bunny.new(:hostname => ip)
    @conn.start

    @ch   = @conn.create_channel
    @msg_queue    = @ch.queue('hello')
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
        temp = Time.now
        if time_diff_milli(@last_timestamp, temp) > @timeout_milli
          puts 'timeout'
          @last_timestamp = temp

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

dc1 = DataCenter.new('dc1', '169.231.10.109')
dc1.run
puts dc1.timeout.to_s

