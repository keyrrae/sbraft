#!/usr/bin/ruby
require_relative 'data_center'

dc1 = DataCenter.new('dc1','169.231.10.109')
t1 = Thread.new do
  dc1.run
end


while true
  sleep(5)
end