require 'forwardable'
require_relative './state_module'

class StateContext
  extend Forwardable

  attr_accessor(:current_state)

  def_delegators :@current_state, :respond_to_append_entries, :respond_to_vote_request, :run

  def initialize(datacenter, leader=false)
    if(leader)
      self.current_state = Leader.new(datacenter, self)
    else
      self.current_state = Follower.new(datacenter, self)
    end
  end

  def set_state(new_state)
    self.current_state = new_state
  end

end
