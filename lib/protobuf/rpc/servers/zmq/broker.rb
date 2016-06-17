require 'thread'

module Protobuf
  module Rpc
    module Zmq
      class Broker
        include ::Protobuf::Rpc::Zmq::Util

        attr_reader :local_queue

        def initialize(server)
          @server = server

          init_zmq_context
          init_local_queue
          init_frontend_socket
          init_poller
        rescue
          teardown
          raise
        end

        def empty?
          local_queue.empty?
        end

        def run
          @idle_workers = []
          @running = true

          loop do
            rc = @poller.poll(broker_polling_milliseconds)

            # The server was shutdown and no requests are pending
            break if rc == 0 && !running? && @server.workers.empty?
            # Something went wrong
            break if rc == -1

            check_and_process_frontend
          end
        ensure
          teardown
          @running = false
        end

        def running?
          @running && @server.running?
        end

        def write_to_frontend(frames)
          @frontend_socket_mutex.synchronize do
            zmq_error_check(@frontend_socket.send_strings(frames))
          end
        end

      private

        def broker_polling_milliseconds
          @broker_polling_milliseconds ||= [ENV["PB_ZMQ_BROKER_POLLING_MILLISECONDS"].to_i, 500].max
        end

        def check_and_process_frontend
          readables_include_frontend = @poller.readables.include?(@frontend_socket)
          message_count_read_from_frontend = 0

          while readables_include_frontend && message_count_read_from_frontend < frontend_poll_weight
            message_count_read_from_frontend += 1
            process_frontend
            break unless local_queue_available? # no need to read frontend just to throw away messages, will prioritize backend when full
            @poller.poll_nonblock
            readables_include_frontend = @poller.readables.include?(@frontend_socket)
          end
        end

        def frontend_poll_weight
          @frontend_poll_weight ||= [ENV["PB_ZMQ_SERVER_FRONTEND_POLL_WEIGHT"].to_i, 1].max
        end

        def init_frontend_socket
          @frontend_socket_mutex = ::Mutex.new
          @frontend_socket = @zmq_context.socket(ZMQ::ROUTER)
          zmq_error_check(@frontend_socket.bind(@server.frontend_uri))
        end

        def init_local_queue
          @local_queue = ::Queue.new
        end

        def init_poller
          @poller = ZMQ::Poller.new
          @poller.register_readable(@frontend_socket)
        end

        def init_zmq_context
          @zmq_context = @server.zmq_context
        end

        def local_queue_available?
          local_queue.size < local_queue_max_size && running?
        end

        def local_queue_max_size
          @local_queue_max_size ||= [ENV["PB_ZMQ_SERVER_QUEUE_MAX_SIZE"].to_i, 5].max
        end

        def process_frontend
          address, _, message, *frames = read_from_frontend

          if message == ::Protobuf::Rpc::Zmq::CHECK_AVAILABLE_MESSAGE
            if local_queue_available?
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::WORKERS_AVAILABLE])
            else
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::NO_WORKERS_AVAILABLE])
            end
          else
            local_queue << [address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, message].concat(frames)
          end
        end

        def read_from_frontend
          frames = []
          @frontend_socket_mutex.synchronize do
            zmq_error_check(@frontend_socket.recv_strings(frames))
          end
          frames
        end

        def teardown
          @frontend_socket.try(:close)
        end
      end
    end
  end
end
