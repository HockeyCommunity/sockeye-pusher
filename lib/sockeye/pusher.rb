require 'timeout'
require 'json'
require 'websocket-client-simple'

module Sockeye
  class Pusher

    attr_accessor :server_address, :secret_token

    def initialize(server_address:, secret_token:)
      self.server_address = server_address
      self.secret_token   = secret_token
    end

    def deliver(identifiers:, payload:)

      # Pull out the class instance varialbe, since the `self` 
      # scope is different once inside a connection block
      #
      secret_token = self.secret_token

      # Wrapping this in a timeout allows the code to gracefully fail
      # quickly in the event of a missing server or such like
      #
      begin
        return Timeout.timeout(5) do
          
          delivered  = false
          error      = false
          connection = WebSocket::Client::Simple.connect self.server_address

          connection.on :open do
            connection.send(
              { 
                action:       :deliver, 
                payload:      payload,
                identifiers:  identifiers,
                secret_token: secret_token
              }.to_json
            )
          end

          connection.on :message do |msg|
            delivered = true
          end

          connection.on :error do |e|
            error = e
          end

          # By waiting for delivered status, we make this async method syncronous.
          # This wait doesn't actually delay the sending from happening, but does
          # potentially delay the method return.
          #
          while !delivered && !error do
            sleep 0.01
          end
          return delivered

        end

      # Simply return false for timeout errors
      #
      rescue Timeout::Error
        return false
      end

    end

  end
end
