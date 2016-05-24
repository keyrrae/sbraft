require 'bunny'
require 'thread'

conn = Bunny.new(:hostname => '169.231.10.109')
conn.start

ch   = conn.create_channel

class RpcClient
  attr_reader :reply_queue
  attr_accessor :response, :call_id
  attr_reader :lock, :condition

  def initialize(ch, server_queue)
    @ch             = ch
    @x              = ch.default_exchange

    @server_queue   = server_queue
    ##randomly_generated_queue
    @reply_queue    = ch.queue("", :exclusive => true)

    @lock      = Mutex.new
    @condition = ConditionVariable.new
    that       = self

    @reply_queue.subscribe do |delivery_info, properties, payload|
      if properties[:correlation_id] == that.call_id
        that.response = payload.to_i
        that.lock.synchronize{that.condition.signal}
      end
    end
  end

  def call(n)
    self.call_id = self.generate_uuid

    @x.publish(n.to_s,
               :routing_key    => @server_queue,
               :correlation_id => call_id,
               :reply_to       => @reply_queue.name)

    lock.synchronize{condition.wait(lock)}
    response
  end

  protected

  def generate_uuid
    # very naive but good enough for code
    # examples
    "#{rand}#{rand}#{rand}"
  end
end

client   = RpcClient.new(ch, "receive_request_queue")
puts " [x] Requesting fib(30)"
response = client.call(30)
puts " [.] Got #{response}"

ch.close
conn.close