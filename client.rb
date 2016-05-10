class Client
end


def printCommands
  puts '================================================='
  puts 'post(p) <message>'
  puts '  - Post a message in DS-blog'
  puts ''

  puts 'lookup(l)'
  puts '  - Display the posts in DS-blog in casual order'
  puts ''

  puts 'exit(e)'
  puts '  - Exit program'
  puts '================================================='
  STDOUT.flush
end


def run
  printCommands

  print '> '
  while true
    cmd = gets

    cmdParse = cmd.strip.split(' ', 2)


    if cmd == '\n'
      print '> '
      next
    end

    case cmdParse[0]
      when 'post' , 'p'
        if cmdParse.length != 2
          puts 'Empty message'
        else
          puts 'Posting message: ' + cmdParse[1]
          # TODO: post message
        end

      when 'lookup' , 'l'
        puts 'Looking up'
      # TODO: look up function

      when 'exit' , 'e'
        puts 'Exiting'
        exit(0)

      when 'help', 'h'
        printCommands
      else
        puts 'Wrong command, type \"help\" or \"h\" for help.'
    end
    print '> '
  end
end


t1=Thread.new{run()}
t1.join
