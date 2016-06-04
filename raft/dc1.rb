require_relative 'data_center'

dc1 = DataCenter.new('dc1','169.231.10.109')
t1 = Thread.new do
  dc1.run
end


dc1.add_log_entry('Message1')
dc1.add_log_entry('Message2')
dc1.add_log_entry('Message3')
dc1.add_log_entry('Message4')

t2 = Thread.new do
  sleep(20)
  puts 'Now I am going to commit some real shit'
  dc1.add_log_entry('Message5')
  dc1.add_log_entry('Message6')
  puts 'Add finished'

end

t2.join
t1.join


