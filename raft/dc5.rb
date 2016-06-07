#!/usr/bin/ruby

require_relative 'data_center'
dc3 = DataCenter.new('dc5','169.231.10.109')
t3 = Thread.new do
  dc3.run
end

while true
  sleep(5)
end