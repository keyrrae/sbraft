require './state'
class Follower < State
  def run
    t1 = Thread.new do
      # Wait for timeout
      while true
        if @election_timer.timeout?
          puts 'timeout'
          @state_context.set_state(Candidate.new(@datacenter, @state_context))
          break
        end
      end # while
    end
    t1.join
  end

  def respond_to_append_entries(append_entries_rpc)
    raise 'Not implemented'
  end

  def respond_to_vote_request(vote_request_rpc)
    raise 'Not implemented'
  end

end
