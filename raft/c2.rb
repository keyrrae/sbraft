#!/usr/bin/ruby
require_relative 'client'
c = Client.new('169.231.10.109', 'dc2')
t = Thread.new do
  c.run
end

while true
  sleep(5)
end