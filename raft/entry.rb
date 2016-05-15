class Entry
  attr_accessor(:index, :term, :type, :message)

  def initialize(index, term, type, message)
    self.index = index
    self.term = term
    self.type = type
    self.message = message

  end

  def is_committed
    self.type == :accepted
  end

  def to_s
    "#{self.index} #{self.term} #{self.type} #{self.message}\n"
  end

end