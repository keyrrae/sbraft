class State
  def initialize(datacenter_context, state_context)
    @datacenter = datacenter_context
    @state_context = state_context
    @election_timer = VoteTimer.new  # Reset Election Timer

  end

  def respond_to_append_entries(append_entries_rpc)
    raise 'Not implemented'
  end

  def respond_to_vote_request(vote_request_rpc)
    raise 'Not implemented'
  end
end

