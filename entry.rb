class Entry
  def initialize(term, committed, message)
    @message = message
    @term = term
    @committed = committed

  end

  def get_term
    @term
  end

  def get_message
    @message
  end

  def is_committed
    @committed
  end

end