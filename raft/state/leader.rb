require './state'
class Leader < State

  attr_accessor :heartbeat_timer

  def initialize(datacenter_context, state_context)
    super(datacenter_context, state_context)
    @heartbeat_timer = Timer.new(Misc::)
  end


  def run
    # As leader, start threads for AppendEntries RPC
    threads = []
    @datacenter.peers.each do |peer|
      threads << Thread.new do
        loop do
          #If RPC timeout, send another wave of AppendEntries
          if @rpc_timeout_timer.timeout?
            begin
              response = nil
              Timeout.timeout(5) do
                response = @datacenter.rpc_appendEntries(peer)
              end
              puts "Peer #{peer.name} respond to appendEntries rpc with: #{response}"
            rescue Timeout::Error
              # puts e.to_s
              puts "Peer #{peer.name} cannot be reached by appendEntries rpc"
              next
            end
          end
        end
      end
    threads.each do |thread|
      thread.join
    end



    # t1 = Thread.new do
    #   while true
    #     if @election_timer.timeout?
    #       @state_context.set_state(Candidate.new(@datacenter, @state_context))
    #       break
    #     end
    #   end
    #
    # end
    #
    # t2 = Thread.new do
    #   while true
    #     if @heart_timer.timeout?
    #       # TODO: send requestVote RPC to peers
    #     end
    #   end
    # end

    t1.join
    t2.join
  end
  #
  #Upon election: send initial empty AppendEntries RPCs
  #(heartbeat) to each server; repeat during idle periods to
  #prevent election timeouts (§5.2)
  #• If command received from client: append entry to local log,
  #                                                         respond after entry applied to state machine (§5.3)
  #• If last log index ≥ nextIndex for a follower: send
  #AppendEntries RPC with log entries starting at nextIndex
  #• If successful: update nextIndex and matchIndex for
  #                                                follower (§5.3)
  #  • If AppendEntries fails because of log inconsistency:
  #                                              decrement nextIndex and retry (§5.3)
  #  • If there exists an N such that N > commitIndex, a majority
  #  of matchIndex[i] ≥ N, and log[N].term == currentTerm:
  #      set commitIndex = N (§5.3, §5.4).

  def respond_to_append_entries(delivery_info, properties, payload)
    raise 'Not implemented'
  end

  def respond_to_vote_request(delivery_info, properties, payload)
    raise 'Not implemented'
  end

end
