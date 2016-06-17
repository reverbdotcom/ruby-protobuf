require 'protobuf/rpc/server'
require 'protobuf/rpc/servers/zmq/util'
require 'thread'

module Protobuf
  module Rpc
    module Zmq
      class Worker
        include ::Protobuf::Rpc::Server
        include ::Protobuf::Rpc::Zmq::Util

        ##
        # Constructor
        #
        def initialize(server, broker)
          @server = server
          @broker = broker
        end

        ##
        # Instance Methods
        #
        def process_request
          client_address, _, data = @broker.local_queue.pop
          return unless data

          ::Thread.current[:busy] = true

          gc_pause do
            encoded_response = handle_request(data)
            @broker.write_to_frontend([client_address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, encoded_response])
          end

          ::Thread.current[:busy] = false
        end

        def run
          loop { process_request }
        end
      end
    end
  end
end
