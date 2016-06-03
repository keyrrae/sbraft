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
              append_entries_reply = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                append_entries_reply = @datacenter.rpc_appendEntries(peer)
              end
              @logger.info "Peer #{peer.name} respond to appendEntries rpc with: #{reply}"

              handle_appendEntries_reply append_entries_reply
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


  # @param append_entries_reply[term, success, match_index, from]
  # @description: Leader handle appendEntries reply from peer
  # 1. Change match_index if success, decrement next_index if failed
  # 2. Commit if enough peer agree on this entry
  def handle_appendEntries_reply(append_entries_reply)
    peer = peers.values[append_entries_reply['from']]
    if append_entries_reply['success']
      peer.match_index = append_entries_reply['match_index']
    else
      peer.next_index = peer.next_index - 1
    end
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


end
