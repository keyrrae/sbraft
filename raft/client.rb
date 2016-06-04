require 'bunny'
require 'json'
require_relative 'misc'

class Client
  attr_accessor(:request_queue, :counter)

  def initialize(ip, dc_name)

    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch = @conn.create_channel

    @dc_client_post_queue_name = "#{dc_name}_client_post_queue"
    @dc_client_lookup_queue_name = "#{dc_name}_client_lookup_queue"

    # ClientPostDirect
    @client_post_direct_exchange = @ch.direct(Misc::CLIENT_POST_DIRECT_EXCHANGE)

    # ClientLookupDirect
    @client_lookup_direct_exchange = @ch.direct(Misc::CLIENT_LOOKUP_DIRECT_EXCHANGE)

    self.request_queue = []
    @leader = nil
    @servers = []
    self.counter = 1 # unique monotonically increasing id, 0 for lookup request
    @cmd = CommandInterface.new(self)
  end

  #TODO
  def client_post_rpc(message)
    puts "#{message}"
    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    call_id = Misc::generate_uuid
    puts "#{call_id}"
    reply_queue.bind(@client_post_direct_exchange, :routing_key => reply_queue.name)

    @client_post_direct_exchange.publish(message,
                                         :routing_key => @dc_client_post_queue_name,
                                         :correlation_id => call_id,
                                         :reply_to => reply_queue.name)
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

  #TODO
  def client_lookup_rpc
    ch = @conn.create_channel
    reply_queue  = ch.queue('', :exclusive => true)
    call_id = Misc::generate_uuid
    reply_queue.bind(@client_lookup_direct_exchange, :routing_key => reply_queue.name)

    @client_lookup_direct_exchange.publish('',
                                           :routing_key => @dc_client_lookup_queue_name,
                                           :correlation_id => call_id,
                                           :reply_to => reply_queue.name)
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

  def run
    # Producer
    t1 = Thread.new do
      # Start command line
      @cmd.run
    end

    t1.join
  end

  def push_request(request)
    self.request_queue << request
  end

  def pop_request
    if self.request_queue.empty?
      nil
    elsif
    req << self.request_queue
      req
    end
  end

end

class CommandInterface
  def initialize(client_context)
    @client = client_context
  end

  def print_commands
    puts '================================================='
    puts 'post(p) <message>'
    puts '  - Post a message in DS-blog'
    puts ''

    puts 'lookup(l)'
    puts '  - Display the posts in DS-blog in casual order'
    puts ''

    puts 'help(h)'
    puts '  - Commands'
    puts ''

    puts 'exit(e or quit or q)'
    puts '  - Exit program'
    puts '================================================='
    STDOUT.flush
  end


  def run
    print_commands

    print '> '
    STDOUT.flush

    while true
      cmd = gets
      cmd_parsed = cmd.strip.split(' ', 2)

      if cmd.match("^\n$")
        print '> '
        STDOUT.flush
        next
      end

      case cmd_parsed[0]
        when 'post' , 'p'
          if cmd_parsed.length != 2
            puts 'Empty message'
          else
            puts 'Posting message: ' + cmd_parsed[1]
            # TODO: post message
            response = @client.client_post_rpc(cmd_parsed[1])
            puts response
            @client.counter += 1
            #@ch.default_exchange.publish(cmd_parsed[1], :routing_key => @msg_queue.name)
          end

        when 'lookup' , 'l'
          puts 'Looking up'
          # TODO: look up function
          response = @client.client_lookup_rpc
          puts response
        when 'exit' , 'e', 'quit', 'q'
          puts 'Exiting'
          exit(0)

        when 'help', 'h'
          print_commands
        else
          puts 'Wrong command, type "help" or "h" for help.'
      end
      print '> '
      STDOUT.flush
      sleep(Misc::CLIENT_CMD_SLEEP_TIME)
    end

  end

end


class ClientRequest
  attr_accessor(:id, :command, :type)

  def initialize(id, command, type)
    self.id = id
    self.command = command
    self.type = type
  end

  def is_lookup?
    self.type == 'lookup'
  end

  def is_post?
    self.type == 'post'
  end

  def to_json
    hash = {}
    hash['id'] = self.id.to_s
    hash['command'] = self.command
    hash['type'] = self.type.to_s

    JSON.dump(hash)
  end

  def self.parse_json(string)
    hash = JSON.load(string)

    self.new(hash['id'].to_i, hash['command'], hash['type'])
  end
end


c = Client.new('169.231.10.109', 'dc2')
c.run


=begin
conn = Bunny.new(:hostname => "169.231.10.109")
conn.start
ch   = conn.create_channel
q    = ch.queue("hello")
ch.default_exchange.publish("Hello World!", :routing_key => q.name)
puts " [x] Sent 'Hello World!'"

t1=Thread.new{run()}
t1.join
con.close
=end