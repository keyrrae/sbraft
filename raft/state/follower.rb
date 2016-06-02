require_relative './state_module'


class Follower < State

  def initialize(datacenter_context)
    super(datacenter_context)
    @logger = Logger.new($stdout)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{@datacenter.name}(Follower): #{msg}\n"
    end
  end

  def run
    @logger.info 'Follower state start'
    loop do
      #Break out the loop and state come to end if state got killed
      if @status == Misc::KILLED_STATE
        @logger.info 'Exit follower state'
        Thread.stop
      end

      if @election_timer.timeout?
        @logger.info 'Follower time out. To candidate state'
        @datacenter.change_state (Candidate.new(@datacenter))
      end
      sleep(Misc::STATE_LOOP_INTERVAL)
    end


  end

  def respond_to_append_entries(delivery_info, properties, payload)
    @election_timer.reset_timer
    @datacenter.append_entries_direct_exchange.publish("#{@datacenter.name} received appendEntries",
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end

  # @description
  # @param payload[:term,:candidate_name,:last_log_index,:last_log_term] CAUTION: Cannot use symbol, must use string as key e.g: payload['term']
  # @sent_message request_vote_reply[:term, :granted, :from]
  def respond_to_vote_request(delivery_info, properties, payload)
    payload = JSON.parse(payload)
    request_vote_reply = {}

    request_vote_reply[:granted] = grant_vote?(payload)

    #Update term and voted_for
    if request_vote_reply[:granted]
      @datacenter.change_term payload['term']
      @datacenter.voted_for = payload['candidate_name']
      @election_timer.reset_timer
    end

    request_vote_reply[:term] = @datacenter.current_term
    request_vote_reply[:from] = @datacenter.name
    @logger.info "Reply RequestVoteRPC from #{payload['candidate_name']} with : #{request_vote_reply[:granted]}"
    @datacenter.request_vote_direct_exchange.publish(request_vote_reply.to_json,
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)
  end

  # @description Should I grant vote?
  # @param payload[:term,:candidate_name,:last_log_index,:last_log_term]
  def grant_vote?(payload)
    if @datacenter.current_term > payload['term']
      return false

    elsif @datacenter.current_term == payload['term']
      # This term already voted
      return false if @datacenter.voted_for != nil
      # 5.4.1 Election restriction
      if @datacenter.last_log_term < payload['last_log_term']
        return true
      elsif @datacenter.last_log_term == payload['last_log_term']
        return true if @datacenter.last_log_index <= payload['last_log_index']
        return false
      else
        return false
      end

    else
      return true
    end

  end


end
