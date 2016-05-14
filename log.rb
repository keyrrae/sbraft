class Entry

  attr_accessor(:index, :term, :type, :message)

  def initialize(index, term, type, message)
    self.index = index
    self.term = term
    self.type = type
    self.message = message

  end

  def is_committed
    self.type == :accepted
  end

  def to_s
    "#{self.index} #{self.term} #{self.type} #{self.message}\n"
  end

end

class Log

  def initialize(datacenter_name)
    @filename = "raft-#{datacenter_name}.log"
    @log_array = []
    @index = 0
    retrieve_log_from_disk
  end

  def retrieve_log_from_disk
    begin
      file = File.open(@filename,'r')
      puts "Found existing log #@filename"

      file.readlines.each do |line|
        parsed_line = line.strip.split(' ', 3)
        index = parsed_line[0].to_i
        term = parsed_line[1].to_i

        type = parsed_line[2].to_i == 0 ? :prepare : :accepted

        entry = Entry.new(index, term, type, parsed_line[2])
        @log_array << entry
        @index += 1
      end

      return true

    rescue
      return false
    end
  end

  def save_log_to_disk
    begin

      file = File.open(@filename, 'w')
      @log_array.each do |entry|
        file.write(entry.to_s)
      end
    rescue
      puts 'File write exception'
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

  def add_entry(term, type, message)
    if type == :prepare
      @index += 1
      @log_array << Entry.new(@index, term, type, message)
    elsif type == :accepted
      # TODO : update condition
      if @log_array[-1].type == :prepare
        @log_array[-1].type = :accepted
      end
    end

  end
end


log = Log.new('dc1')
=begin
log.add_entry(1, :prepare, 'hello')
log.add_entry(1, :accepted, 'hello')
log.add_entry(2, :prepare, 'world')
log.add_entry(2, :accepted, 'world')
log.save_log_to_disk
=end
log.print_log
