require_relative 'data_center'
dc3 = DataCenter.new('dc4','169.231.10.109')
t3 = Thread.new do
  dc3.run
end

t3.join