require_relative 'data_center'

dc1 = DataCenter.new('dc1','169.231.10.109')
t1 = Thread.new do
  dc1.run
end


dc1.add_log_entry('Message1')
dc1.add_log_entry('Message2')
dc1.add_log_entry('Message3')
dc1.add_log_entry('Message4')

t1.join


