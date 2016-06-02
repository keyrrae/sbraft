require_relative './state_module'

class Follower < State

  def run
    puts "#{@datacenter.name}'s Follower state start"
    loop do
      #Break out the loop and state come to end if state got killed
      if @status == Misc::KILLED_STATE
        puts 'Killed'
      end

      if @election_timer.timeout?
        puts "#{@datacenter.name} Follower time out. To candidate state"
        @datacenter.change_state (Candidate.new(@datacenter))
      end
      sleep(Misc::STATE_LOOP_INTERVAL)
    end

    puts "#{@datacenter.name}'s Follower state end"

  end

  def respond_to_append_entries(delivery_info, properties, payload)
    @election_timer.reset_timer
    @datacenter.append_entries_direct_exchange.publish("#{@datacenter.name} received appendEntries",
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end

  def respond_to_vote_request(delivery_info, properties, payload)
    puts payload
    @datacenter.request_vote_direct_exchange.publish("#{@datacenter.name} voted for you",
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end


end
