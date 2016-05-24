require 'bunny'

conn = Bunny.new(:hostname => '169.231.10.109')
conn.start

ch = conn.create_channel
class RpcServer
  def initialize(dc_name, all_dc_names, ch)
    @ch = ch
    @dc_name = dc_name
    @all_dc_names = all_dc_names
    @timer = Timer.new
    @all_dc_append_receiver_queue_list = @all_dc_names.map do |dc_name|
      "#{dc_name}_appendentries_receiver"
    end
  end

  def send_append_entries

  end

  def start
    #start queue listening client's request

    # @q = @ch.queue(queue_name)
    # Bind to a fanout exchange using anonymous queue
    @x = @ch.direct('AppendEntriesDirect')

    @append_receiver_queue = @ch.queue("#{@dc_name}_appendentries_receiver")
    @append_receiver_queue.bind(@x, :routing_key=> "#{@dc_name}_appendentries_receiver")


    # For receiving AppendEntries from other data centers
      @append_receiver_queue.subscribe do |delivery_info, properties,payload|
        n = payload.to_s
        r = "#{@dc_name} received your append entries request"
        # reply back
        puts 'Received Append Entries'
        @x.publish(r, :routing_key => properties.reply_to)
      end



    #Used for getting rpc reply back
    @reply_receiver_queue = @ch.queue('', :exclusive => true)
    @reply_receiver_queue.bind(@x, :routing_key => @reply_receiver_queue.name )
    #Listen to rpc replies
      @reply_receiver_queue.subscribe do |delivery_info, properties, payload|
        puts payload
      end


    # Send append entries rpc to other data centers
    while true
      sleep(0.5)
      if @timer.timeout?
        @all_dc_append_receiver_queue_list.each do |receiver_queue_name|
          @x.publish("#{@dc_name} append entries",:routing_key => receiver_queue_name, :reply_to => @reply_receiver_queue.name)
        end
      end
    end


  end

  def generate_uuid
    # very naive but good enough for code
    # examples
    "#{rand}#{rand}#{rand}"
  end
end


class DataCenterAppendEntriesRpcInfo
  attr_accessor( :correlation_id, :lock)
end


class Timer

  attr_accessor(:last_timestamp, :timeout_milli)

  def initialize
    self.timeout_milli = rand(1000..3000)
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



begin
  puts 'Enter data center name'
  center_name = gets.strip
  puts 'Enter data centers splited by space'
  centers = gets.strip.split(' ')
  dc1 = RpcServer.new(center_name, centers,ch)
  # dc2 = RpcServer.new(:dc2, [:dc1,:dc2],ch2)

  'Awaits apc request'
  dc1.start
  # dc2.start

rescue Interrupt => _
  ch.close
  conn.close
end