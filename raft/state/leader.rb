require_relative './state_module'

class Leader < State

  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{@datacenter.name}(Leader): #{msg}\n"
    end
  end
  def run
    @logger.info 'Leader state start'
    # As leader, start threads for AppendEntries RPC
    threads = []
    @datacenter.peers.values.each do |peer|
      threads << Thread.new do
        loop do
          #Break out the loop and state come to end if state got killed
          if @status == Misc::KILLED_STATE
            Thread.stop
          end

          #If HeartBeatTimer timeout, send another wave of AppendEntries
          if peer.heartbeat_timer.timeout?
            peer.heartbeat_timer.reset_timer
            begin
              response = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                response = @datacenter.rpc_appendEntries(peer)
              end
              @logger.info "Peer #{peer.name} respond to appendEntries rpc with: #{response}"
              ##得到response后，应该做：1。检查结果，正确的话增加machIndex和nextIndex,错误的话回滚nextIndex，matchIndex不变！

            rescue Timeout::Error
              @logger.info "Peer #{peer.name} cannot be reached by appendEntries rpc"
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
    @logger.info 'Leader state end'
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
    #
    # t1.join
    # t2.join
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


end
