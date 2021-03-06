require 'pstore'
require 'pathname'
module Config

  # @description: Read current_peers_set from config.txt
  def read_config
    @cfg_filename = Misc::ROOT_DIR + '/configuration.txt'
    file = File.open(@cfg_filename, 'r')
    @logger.info "Read configuration file #{@cfg_filename}"
    file.readlines.each do |line|
      dc_name = line.strip
      @current_peers_set << dc_name
    end
  end

  # @description: Persistent state: [current_term, voted_for, logs]
  def read_storage
    #Read Pstore file from persistent storage if there is one.
    if File.exist? "#{@name}.pstore"
      @logger.info 'Found previous storage. Read PStore'
      @store = PStore.new("#{@name}.pstore")
      @store.transaction do
        @current_term = @store[:current_term]
        @voted_for = @store[:voted_for]
        @logs = @store[:logs]
      end
    else
      @logger.info 'Previous Storage not found. Create one.'
      @store = PStore.new("#{@name}.pstore")

      # Create an empty log. Log's index start at 1 and add a stub
      @logs = [LogContainer::LogEntry.new(0,Misc::COMMITTED,'Empty', Set.new([]), Set.new([]))]

      @store.transaction do
        @store[:current_term] = 1
        @store[:voted_for] = nil
        @store[:logs] = @logs
      end
    end
  end
end