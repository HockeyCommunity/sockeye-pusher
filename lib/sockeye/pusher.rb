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

      Rails.logger.debug "deliver_request"
      Rails.logger.debug "identifiers"
      Rails.logger.debug identifiers
      Rails.logger.debug "payload"
      Rails.logger.debug payload
      Rails.logger.debug "server_address"
      Rails.logger.debug self.server_address
      Rails.logger.debug "secret_token"
      Rails.logger.debug self.secret_token

      # Pull out the class instance varialbe, since the `self` 
      # scope is different once inside a connection block
      #
      secret_token = self.secret_token

      # Wrapping this in a timeout allows the code to gracefully fail
      # quickly in the event of a missing server or such like
      #
      begin
        return Timeout.timeout(5) do
          
          Rails.logger.debug "opening socket..."

          delivered  = false
          error      = false
          connection = WebSocket::Client::Simple.connect self.server_address

          connection.on :open do
            Rails.logger.debug "socket open. sending..."
            connection.send(
              { 
                action:       :deliver, 
                payload:      payload,
                identifiers:  identifiers,
                secret_token: secret_token
              }.to_json
            )
            Rails.logger.debug "SENT!"
          end

          connection.on :message do |msg|
            Rails.logger.debug "RESPONSE!"
            Rails.logger.debug msg.inspect
            delivered = true
          end

          connection.on :error do |e|
            Rails.logger.debug "ERROR!"
            error = e
          end

          # By waiting for delivered status, we make this async method syncronous.
          # This wait doesn't actually delay the sending from happening, but does
          # potentially delay the method return.
          #
          while !delivered && !error do
            sleep 0.01
          end
          Rails.logger.debug "FINISHED!"
          Rails.logger.debug delivered
          return delivered

        end

      # Simply return false for timeout errors
      #
      rescue Timeout::Error
        Rails.logger.debug "TIMEOUT!"
        return false
      end

    end

  end
end
