
class Log

  def initialize(datacenter_name)
    @filename = "raft-#{datacenter_name}.log"
    @log_array = []
    @index = 0
    retrieve_log_from_disk

  end

  def get_last_term
    @log_array.last.term
  end

  def get_last_index
    @log_array.length + 1
  end

  def retrieve_log_from_disk
    begin
      file = File.open(@filename,'r')
      puts "Found existing log #{@filename}"

      file.readlines.each do |line|
        parsed_line = line.strip.split(' ', 2)

        term = parsed_line[0].to_i

        type = parsed_line[1].to_i == 0 ? :prepare : :accepted

        entry = Entry.new(term, type, parsed_line[2])
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

      @log_array << Entry.new(term, type, message)
    elsif type == :accepted
      # TODO : update condition
      if @log_array[-1].type == :prepare
        @log_array[-1].type = :accepted
      end
    end

  end

end


class Entry
  attr_accessor(:term, :type, :message)

  def initialize(term, type, message)

    self.term = term
    self.type = type
    self.message = message

  end

  def is_committed
    self.type == :accepted
  end

  def to_s
    "#{self.term} #{self.type} #{self.message}\n"
  end

end


=begin
log = Log.new('dc1')
log.add_entry(1, :prepare, 'hello')
log.add_entry(1, :accepted, 'hello')
log.add_entry(2, :prepare, 'world')
log.add_entry(2, :accepted, 'world')
log.save_log_to_disk
log.print_log
=end

