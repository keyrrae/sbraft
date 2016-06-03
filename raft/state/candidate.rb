require_relative './state_module'

class Candidate < State
  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{@datacenter.name}(Candidate): #{msg}\n"
    end
    # Increment Datacenter Term and reset voted_for
    @datacenter.new_term
    @datacenter.voted_for = @datacenter.name
  end

  def run
    @logger.info 'Candidate state start'
    threads = []

    @datacenter.peers.values.each do |peer|
      threads << Thread.new do
        loop do
          if @status == Misc::KILLED_STATE
            Thread.stop
          end
          if !peer.queried
            begin
              request_vote_reply = nil
              Timeout.timeout(Misc::RPC_TIMEOUT) do
                request_vote_reply = @datacenter.rpc_requestVote(peer)
              end
              peer.queried = true
              handle_requestVote_reply request_vote_reply
            rescue Timeout::Error
              @logger.info "RequestVoteRPC to #{peer.name} timeout"
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
    @logger.info 'Candidate state end'
  end

  def respond_to_append_entries(delivery_info, properties, payload)

  end

  def respond_to_vote_request(delivery_info, properties, payload)
  end



  # @param request_vote_reply: [term, granted, from]
  # @description: Candidate handle requestVote reply from
  # follower(or candidate/leader, which may cause step down)
  # 1. If receive higher term, change term and step down
  # 2. If reply's term comply with sent term, and reply is true,
  # add one quorum and check if can step up(enough quorum)

  def handle_requestVote_reply(request_vote_reply)
    request_vote_reply = JSON.parse(request_vote_reply)
    term = request_vote_reply['term']

    # Step 1
    if term > @datacenter.current_term
      @datacenter.current_term = term
      @datacenter.change_state (Follower.new(@datacenter))
    end

    # Step 2
    if term == @datacenter.current_term && request_vote_reply['granted']
      @datacenter.peers[request_vote_reply['from']].vote_granted = true
      @logger.info "Collect one quorum from #{request_vote_reply['from']}"
      if @datacenter.enough_quorum?
        @logger.info 'Got enough quorum. change state to leader'
        @datacenter.change_state(Leader.new(@datacenter))
      end
    end
  end

end