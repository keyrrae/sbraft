require 'test/unit'
require_relative 'test_helper'

class TestSuites < Test::Unit::TestCase
  def test_leader_election
    dc2 = DataCenter.new('dc2','169.231.10.109')
    t2 = Thread.new do
      dc2.run
    end
    sleep(8)
    dc3= DataCenter.new('dc3','169.231.10.109')
    t3 = Thread.new do
      dc3.run
    end

    t2.join
    t3.join

  end
end