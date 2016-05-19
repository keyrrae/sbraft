require 'bunny'
require 'json'
require_relative 'misc'

class Client
  attr_accessor(:request_queue, :counter)

  def initialize(ip, queue_name)
    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch   = @conn.create_channel
    self.request_queue = []
    @leader = nil
    @servers = []
    self.counter = 1 # unique monotonically increasing id, 0 for lookup request
    @msg_queue    = @ch.queue(queue_name)
    @cmd = CommandInterface.new(self)
    run
  end

  def run
    # Producer
    t1 = Thread.new do
      # Start command line
      @cmd.run
    end


    # Consumer
    t2 = Thread.new do
      # TODO: client protocol
      while true
        until self.request_queue.empty?
          req = self.pop_request
          tmr = Timer.new(100)

          # TODO: send request to leader

          next_server_index = 0
          while true

            if tmr.timeout?
              next_server = @servers[next_server_index]
              # TODO: send request to next_server
            end
            # TODO: poll response
            # if got a response
            #   if response.leader != @leader
            #     @servers.remove(response.leader)
            #     @servers.add(@leader)
            #     @leader = response.leader
            #   end
            #   if response.type == 'posted'
            #     puts 'posted'
            #     STDOUT.flush
            #   else
            #     puts response.content
            #     STDOUT.flush
            #   end
          end
        end
      end
    end

    t1.join
    t2.join

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
            @client.push_request(ClientRequest.new(@client.counter, cmd_parsed[1], 'post'))
            @client.counter += 1
            #@ch.default_exchange.publish(cmd_parsed[1], :routing_key => @msg_queue.name)
          end

        when 'lookup' , 'l'
          puts 'Looking up'
          # TODO: look up function
          @client.request_queue << ClientRequest.new(0, '', 'lookup')

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


#c = Client.new('169.231.10.109', 'hello')
#c.run


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