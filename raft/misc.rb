module Misc
  class Timer
    attr_accessor(:last_timestamp, :timeout_milli)

    def initialize(timeout)
      self.timeout_milli = timeout
      self.last_timestamp = Time.now
    end

    def set_timeout(timeout)
      self.timeout_milli = timeout
      self.last_timestamp = Time.now
    end

    def reset_timer
      self.last_timestamp = Time.now
    end

    def timeout?
      temp = Time.now
      if time_diff_milli(@last_timestamp, temp) > self.timeout_milli then
        @last_timestamp = temp
        true
      else
        false
      end
    end

    def time_diff_milli(start, finish)
      (finish - start) * 1000.0
    end
  end
  public
  def self.generate_uuid
    # very naive but good enough for code
    # examples
    "#{rand}#{rand}#{rand}"
  end
end