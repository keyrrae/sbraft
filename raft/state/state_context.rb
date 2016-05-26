class StateContext < Forwardable
  attr_accessor(:current_state)

  def_delegators :@current_state, :respond_to_append_entries, :respond_to_vote_request

  def initialize(datacenter)
    self.current_state = Follower.new(datacenter, self)
  end

  def set_state(new_state)
    self.current_state = new_state
  end

end
