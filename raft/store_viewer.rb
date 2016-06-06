require 'pstore'
require_relative "../raft/storage/log_container"

module StoreViewer
def self.read_config(name)
  store = PStore.new("#{name}.pstore")
  puts "#{name}: \n"
  store.transaction do
    puts "Current term: #{store[:current_term]}\n"
    puts "Voted for: #{store[:voted_for]}\n"
    logs = store[:logs]
    logs.each do |log|
      puts "#{log.to_s} \n"
    end
  end
end
end


StoreViewer::read_config('dc1')