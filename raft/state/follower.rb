require_relative './state_module'

class Follower < State

  def run
    puts "#{@datacenter.name}'s Follower state start"
    loop do
      #Break out the loop and state come to end if state got killed
      break if @status == Misc::KILLED_STATE

      if @election_timer.timeout?
        puts 'Follower time out. To candidate state'
        @datacenter.change_state (Candidate.new(@datacenter))
      end

      puts "#{@datacenter.name} is in Follower state"

      sleep(Misc::STATE_LOOP_INTERVAL)
    end

  end

  def respond_to_append_entries(delivery_info, properties, payload)
    @election_timer.reset_timer
    @datacenter.append_entries_direct_exchange.publish("#{@datacenter.name} received appendEntries",
                                                       :routing_key => properties.reply_to,
                                                       :correlation_id => properties.correlation_id)

  end

  def respond_to_vote_request(delivery_info, properties, payload)
    raise 'Not implemented'
  end

end
