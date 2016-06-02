require 'pstore'
module Config
  def read_config
    @cfg_filename = 'configuration.txt'
    file = File.open(@cfg_filename, 'r')
    puts "Read configuration file #{@cfg_filename}"
    file.readlines.each do |line|
      dc_name = line.strip
      if dc_name != self.name
        @peers[dc_name] = Peer.new(dc_name)
      end
    end

  end

  def read_storage
    #Read Pstore file from persistent storage if there is one.
    if File.exist? "#{@name}.pstore"
      puts 'Found previous storage. Read PStore'
      @store = PStore.new("#{@name}.pstore")
      @store.transaction do
        @current_term = @store[:current_term]
        @voted_for = @store[:voted_for]
        @logs = @store[:logs]
        @commit_index = @store[:commit_index]
      end
    else
      puts 'Previous Storage not found. Create one.'
      @store = PStore.new("#{@name}.pstore")
      @store.transaction do
        @store[:current_term] = 1
        @store[:voted_for] = nil
        @store[:logs] = []
        @store[:commit_index] = 0
      end
    end
  end
end