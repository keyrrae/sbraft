class Entry
  def initialize(index, term, type, message)
    @index = index
    @term = term
    @type = type
    @message = message

  end

  def get_term
    @term
  end

  def get_message
    @message
  end

  def get_type
    @type
  end

  def set_type(type)
    @type = type
  end

  def is_committed
    @type == :accepted
  end

end

class Log

  def initialize(datacenter_name)
    @filename = "raft-#{datacenter_name}.log"
    @log_array = []
    @term = 0

  end

  def retrieve_log_from_disk
    begin
      @file = File.open(@filename,'r')
      puts "Found existing log #@filename"

      @file.readlines.each do |line|
        parsed_line = line.strip.split(' ', 2)
        term = parsed_line[0].to_i
        if term > @term
          @term = t
        end

        type = parsed_line[1].to_i == 0 ? :prepare : :accepted

        entry = Entry.new(term, type, parsed_line[2])
        @log_array << entry

      end

      return true

    rescue
      return false
    end
  end


  def print_log

     # log is empty
     if @log_array == []
       puts 'EMPTY'
     else
       puts "term\tcommitted\tmessage"
       @log_array.each do |log_entry|
         puts "#{log_entry}"
       end
     end
  end

  def add_entry(message, type)
    if type == :prepare
      @log_array << Entry.new(@term, type, message)
    elsif type == :accepted

    end

  end
end


