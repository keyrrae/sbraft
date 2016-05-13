class Log
  class Entry
    def initialize(message, term, committed)
      @message = message
      @term = term
      @committed = committed

    end

    def get_term
      @term
    end

    def get_message
      @message
    end

    def is_committed
      @committed
    end

  end

  def initialize(datacenter_name)
    @filename = "raft-#{datacenter_name}.log"

  end

  def retrieve_log_from_disk
    begin
      @file = File.open(@filename,'r')
      @file.readlines.each do |line|
        parsed_line = line.strip.split

      end
    rescue
      puts 'file IO exception'
    end



  end

end
