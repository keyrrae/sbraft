require_relative './state_module'

class Follower < State

  def run
    # t1 = Thread.new do
    #   # Wait for timeout
    #   while true
    #     if @election_timer.timeout?
    #       puts 'timeout'
    #       @state_context.set_state(Candidate.new(@datacenter, @state_context))
    #       break
    #     end
    #   end # while
    # end
    # t1.join
    loop do
      sleep(0.05)
    end
  end

  def respond_to_append_entries(delivery_info, properties, payload)
    @election_timer.reset_timer
    @datacenter.append_entries_direct_exchange.publish("#{@datacenter.datacenter_name} received appendEntries",
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end

  def respond_to_vote_request(delivery_info, properties, payload)
    raise 'Not implemented'
  end

end
