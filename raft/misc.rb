module Misc
  require 'thread'
  class Timer
    attr_accessor(:last_timestamp, :timeout_milli)
    mutex = Mutex.new

    def initialize(timeout)
      self.timeout_milli = timeout
      self.last_timestamp = Time.now
    end

    def set_timeout(timeout)
      self.timeout_milli = timeout
      self.last_timestamp = Time.now
    end

    def reset_timer
      #Avoid read timeout when resetting
      mutex.synchronize {
        self.last_timestamp = Time.now
      }
    end

    def timeout?
      mutex.synchronize {
        temp = Time.now
        if time_diff_milli(@last_timestamp, temp) > self.timeout_milli then
          @last_timestamp = temp
          true
        else
          false
        end
      }
    end

    def time_diff_milli(start, finish)
      (finish - start) * 1000.0
    end
  end

  def self.generate_uuid
    # very naive but good enough for code
    # examples
    "#{rand}#{rand}#{rand}"
  end


  #Constants
  ELECTION_TIMEOUT = 10.freeze
  RPC_TIMEOUT = 500.freeze
  APPEND_ENTRIES_DIRECT_EXCHANGE = 'AppendEntriesDirect'.freeze
  VOTE_REQUEST_DIRECT_EXCHANGE = 'VoteRequestDirect'.freeze

end