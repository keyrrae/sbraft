require "bunny"
require_relative "log"

class DataCenter

  attr_accessor(:datacenter_name, :log, :role, :state, :timeout)

  def initialize(datacenter_name)
    self.datacenter_name = datacenter_name
    self.log = Log.new(datacenter_name)
    self.role = :candidate
    self.timeout = rand(300) / 1000.0
    @conn = Bunny.new(:hostname => '169.231.10.109')
    @conn.start

    @ch   = @conn.create_channel
    @msg_queue    = @ch.queue('hello')
    @signal_queue = []


  end

  def run
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
        if @signal_queue.empty?
          continue
        else
          signal = @signal_queue.shift # dequeue from front
          dispatch(signal)
        end #if
      end # while
    end # thread

    t1.join
    t2.join
  end

  def dispatch(signal)
    puts signal
  end
end

dc1 = DataCenter.new('dc1')
dc1.run
puts dc1.timeout.to_s

