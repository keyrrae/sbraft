require 'bunny'

class Client
  attr_accessor(:request_queue)

  def initialize(ip, queue_name)
    @conn = Bunny.new(:hostname => ip)
    @conn.start
    @ch   = @conn.create_channel
    self.request_queue = Quene.new
    @leader = None
    @servers = []
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
    end

    t1.join
    t2.join

  end

  def push_request(request)
    self.request_queue << request
  end

  def pop_request
    if self.request_queue.empty?
      None
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
            @ch.default_exchange.publish(cmd_parsed[1], :routing_key => @msg_queue.name)
          end

        when 'lookup' , 'l'
          puts 'Looking up'
        # TODO: look up function

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

end

class LookupRequest < ClientRequest

end

class PostRequest < ClientRequest

end

c = Client.new('169.231.10.109', 'hello')
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