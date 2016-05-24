require_relative 'data_center'

class Message
  def initialize(datacenter_context, destination_context)
    @type = 'message'
    @datacenter = datacenter_context
    @term = @datacenter.current_term
    @last_log_term = @datacenter.log.get_last_term

  end

  def to_json

  end
end

class RequestVoteMessage
  def initialize(datacenter_context)
    super
    @type = 'request_vote'


  end



end