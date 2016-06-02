require_relative './state_module'
require_relative '../misc'

class Candidate < State
  def initialize(datacenter_context)
    super(datacenter_context)
    @datacenter.new_term  # Increment Datacenter Term
    @vote_count = 1 # Vote for self
    # TODO: send requestVote RPC to peers
  end

  def run
    puts "#{@datacenter.name} candidate state start"
    threads = []



    @datacenter.peers.each do |peer|
      threads << Thread.new do
        loop do
          begin
            break if @status == Misc::KILLED_STATE
            Timeout.timeout(Misc::RPC_TIMEOUT) do
              response = @datacenter.rpc_requestVote(peer)
            end

            puts "Got vote from #{peer.name}"
            break

          rescue Timeout::Error
            puts "RequestVoteRPC to #{peer.name} timeout"
          end
        end
      end
    end

    threads.each do |thread|
      thread.join
    end

    puts "#{@datacenter.name} candidate state end"

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