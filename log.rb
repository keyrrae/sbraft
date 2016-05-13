class Log

  def initialize(datacenter_name)
    @filename = "raft-#{datacenter_name}.log"
    @log_array = []

  end

  def retrieve_log_from_disk

    begin
      @file = File.open(@filename,'r')
      puts "Found existing log #@filename"

      @file.readlines.each do |line|
        parsed_line = line.strip.split(' ', 2)
        @log_array << Entry.new(parsed_line[0], parsed_line[1], parsed_line[2])

      end
    rescue
      puts 'file IO exception'
    end



  end

end
