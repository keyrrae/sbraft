require 'pstore'
module Test
  @name = "shit"
  if File.exist? "#{@name}.pstore"
    puts 'Found previous storage. Read PStore'
    @store = PStore.new("#{@name}.pstore")
    @store.transaction do
      @current_term = @store[:current_term]
      @voted_for = @store[:voted_for]
      @log = @store[:log]

    end
    puts @current_term
    puts @voted_for
    puts @log[0][:message]
  else
    puts 'Previous Storage not found. Create one.'
    @store = PStore.new("#{@name}.pstore")
    @store.transaction do
      @store[:current_term] = 1
      @store[:voted_for] = 1
      @store[:log] = [{:term=>1, :message=>'fucl'},{:term=>2, :message=>'fuck'}]
    end
  end
end
