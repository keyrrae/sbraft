module StateModule
  require_relative './state'
  require_relative './follower'
  require_relative './leader'
  require_relative './candidate'
  require_relative '../misc'
  require 'logger'
  require 'json'
end