require_relative './state_module'
require_relative '../misc'

class Candidate < State
  def initialize(datacenter_context)
    super(datacenter_context)
    # Increment Datacenter Term and reset voted_for
    @datacenter.new_term
    @datacenter.voted_for = @datacenter.name
  end

  def run
    puts "#{@datacenter.name} candidate state start"
    threads = []

    @datacenter.peers.values.each do |peer|
      threads << Thread.new do
        loop do
          break if @status == Misc::KILLED_STATE
          if !peer.queried
            begin
              reply = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                reply = @datacenter.rpc_requestVote(peer)
              end
              puts "#{@datacenter.name} got reply from peer #{peer.name} : #{reply}"
              peer.queried = true
              handle_requestVote_reply reply
            rescue Timeout::Error
              puts "RequestVoteRPC to #{peer.name} timeout"
            end
          else
            sleep(Misc::STATE_LOOP_INTERVAL)
          end
        end
      end
    end

    threads.each do |thread|
      thread.join
    end
    puts "#{@datacenter.name} candidate state end"
  end

  def respond_to_append_entries(delivery_info, properties, payload)

  end

  def respond_to_vote_request(delivery_info, properties, payload)
  end



  # @param reply: [term, granted, from]
  # @return
  def handle_requestVote_reply(reply)
    term = reply[:term]
    granted = reply[:granted]

    #Receive reply with higher term, step down
    if response[:term] > @datacenter.current_term
      @datacenter.current_term = term
      @datacenter.change_state (Follower.new(@datacenter))
    end

    # If term comply with the term candidate sent
    # and the reply is true, add one quorum and check if enough quorum
    if reply[:term] == @datacenter.current_term and reply[:granted]
      @datacenter.peers[reply[:from]].vote_granted
      puts "#{@datacenter.name} collect one quorum"
      if @datacenter.enough_quorum?
        puts "#{@datacenter.name} change state to leader"

        @datacenter.change_state(Leader.new(@datacenter))

      end
    end
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