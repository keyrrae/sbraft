require_relative './state_module'

class State
  attr_accessor :datacenter,
                :election_timer,
                :status #Running or killed

  def initialize(datacenter)
    @datacenter = datacenter
    #Timer for election
    @election_timer = Misc::Timer.new(Misc.RANDOM_ELECTION_TIMEOUT)
    @status = Misc::RUNNING_STATE
  end

  #Need to be handled in specific code. When status is killed,
  #every Thread running in a State(e.g Leader sending AppendEntries) should exit
  def respond_to_append_entries(delivery_info, properties, payload)
    raise 'Not implemented'
  end

  def respond_to_vote_request(delivery_info, properties, payload)
    raise 'Not implemented'
  end

  def run
    raise 'Not implemented'
  end
end

