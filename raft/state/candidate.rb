require_relative './state_module'
require_relative '../misc'

class Candidate < State
  def initialize(datacenter_context)
    super(datacenter_context)
    @heart_timer = Misc::Timer.new(50)
    @datacenter.new_term  # Increment Datacenter Term
    @vote_count = 1 # Vote for self
    # TODO: send requestVote RPC to peers
    run
  end

  def run
    puts "#{@datacenter.name} is candidate state start"
    loop do
      #Break out the loop and state come to end if state got killed
      break if @status == Misc::KILLED_STATE

      sleep(Misc::STATE_LOOP_INTERVAL)
    end

    puts "#{@datacenter.name} is candidate state end"

  end


  # def respond_to_signal(signal)
  #
  #   case signal.downcase
  #     when 'voted for you'
  #       @vote_count += 1
  #       if @vote_count >= @datacenter.quorum
  #         @state_context.set_state(Leader.new(@datacenter))
  #       end
  #     when 'append entries'
  #
  #       #if signal.term > @datacenter.term
  #       #  @datacenter.current_term = signal.term
  #       #  @fsm.current_state.set_state(Follower.new(@datacenter, @fsm))
  #       #end
  #
  #     else
  #       # do nothing
  #   end
  #
  # end
end