class State
  attr_accessor :datacenter, :state_context, :election_timer

  def initialize(datacenter, state_context)
    @datacenter = datacenter
    @state_context = state_context
    #Timer for election
    @election_timer = Timer.new
    run
  end

  def respond_to_append_entries(append_entries_rpc)
    raise 'Not implemented'
  end

  def respond_to_vote_request(vote_request_rpc)
    raise 'Not implemented'
  end

  def run
    raise 'Not implemented'
  end
end

