
module Config
  require 'pstore'
  def read_config_and_storage
    @cfg_filename = 'configuration.txt'
    file = File.open(@cfg_filename, 'r')
    puts "Read configuration file #{@cfg_filename}"
    file.readlines.each do |line|
      dc_name = line.strip
      if dc_name != self.datacenter_name
        @peers << Peer.new(dc_name)
      end
    end

    #Read Pstore file from persistent storage if there is one.
    if File.exist? "#{@datacenter_name}.pstore"
      puts 'Found previous storage. Read PStore'
      @store = PStore.new("#{@datacenter_name}.pstore")
      @store.transaction do
        @current_term = @store[:current_term]
        @voted_for = @store[:voted_for]
        @log = @store[:log]
      end
    else
      puts 'Previous Storage not found. Create one.'
      @store = PStore.new("#{@datacenter_name}.pstore")
      @store.transaction do
        @store = PStore.new("#{@datacenter_name}.pstore")
        @store[:current_term] = 1
        @store[:voted_for] = nil
        @store[:log] = []
      end
    end
  end
end