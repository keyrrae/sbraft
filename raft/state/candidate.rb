

class Candidate < State
  def initialize(datacenter_context, state_context)
    super(datacenter_context, state_context)
    @heart_timer = Timer.new(50)
    @datacenter.new_term  # Increment Datacenter Term
    @vote_count = 1 # Vote for self
    # TODO: send requestVote RPC to peers
    run
  end

  def run

    t1 = Thread.new do
      while true
        if @election_timer.timeout?
          @state_context.set_state(Candidate.new(@datacenter, @state_context))
          break
        end
      end

    end

    t2 = Thread.new do
      while true
        if @heart_timer.timeout?
          # TODO: send requestVote RPC to peers
        end
      end
    end

    t1.join
    t2.join
  end


  def respond_to_signal(signal)

    case signal.downcase
      when 'voted for you'
        @vote_count += 1
        if @vote_count >= @datacenter.quorum
          @state_context.set_state(Leader.new(@datacenter, @state_context))
        end
      when 'append entries'

        #if signal.term > @datacenter.term
        #  @datacenter.current_term = signal.term
        #  @fsm.current_state.set_state(Follower.new(@datacenter, @fsm))
        #end

      else
        # do nothing
    end

  end
end