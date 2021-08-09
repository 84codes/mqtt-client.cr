require "socket"
require "openssl"
require "./mqtt-client/connection"

module MQTT
  class Client
    def initialize(@host : String, @port = 1883, @tls = false, @client_id = "", @clean_session = true, @user = "", @password = "", @keepalive = 60u16)
      @verify_mode = OpenSSL::SSL::VerifyMode::PEER
      @publishes = Channel(Message).new(32)
      @subscriptions = Array(Tuple(String, UInt8)).new
      spawn connect_loop, name: "MQTT Client connect_loop"
      Fiber.yield
    end

    @closed = false
    @connection : Connection?
    @conn_lock = Mutex.new

    def connect_loop
      @conn_lock.lock
      loop do
        break if @closed
        connection = @connection = connect
        connection.on_message = @on_message
        @conn_lock.unlock
        connection.read_loop
        puts "Disconnected from MQTT server: EOFError"
      rescue ex
        puts "Disconnected from MQTT server: #{ex.inspect_with_backtrace}"
      ensure
        if @connection
          @conn_lock.lock
          @connection = nil
        end
        sleep 1
      end
    end

    def connect
      if @tls
        socket = connect_tls(connect_tcp)
        Connection.new(socket, @client_id, @clean_session, @user, @password, nil, @keepalive.to_u16)
      else
        socket = connect_tcp
        Connection.new(socket, @client_id, @clean_session, @user, @password, nil, @keepalive.to_u16)
      end
    end

    def publish(message : Message)
      @conn_lock.synchronize do
        @connection.not_nil!.publish message
      end
    end

    def publish(topic : String, body : Bytes, qos : Int = 0u8, retain = false)
      publish(Message.new(topic, body, qos.to_u8, retain))
    end

    def publish(topic : String, body : String, qos : Int = 0u8, retain = false)
      publish(Message.new(topic, body.to_slice, qos.to_u8, retain))
    end

    def on_message(&blk : Message -> Nil)
      @on_message = blk
    end

    def subscribe(topic : String, qos = 0u8)
      raise ArgumentError.new("No on_message handler set") unless @on_message
      @conn_lock.synchronize do
        @connection.not_nil!.subscribe topic, qos
      end
    end

    def subscribe(topics : Enumerable(Tuple(String, UInt8)))
      raise ArgumentError.new("No on_message handler set") unless @on_message
      # TODO
    end

    def unsubscribe(topic : String, qos = 0u8)
      @conn_lock.synchronize do
        @connection.not_nil!.unsubscribe topic, qos
      end
    end

    private def connect_tcp
      socket = TCPSocket.new(@host, @port, connect_timeout: 30)
      socket.keepalive = true
      socket.tcp_nodelay = false
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.sync = false
      socket.read_buffering = true
      socket.buffer_size = 16384
      socket.read_timeout = @keepalive * 1.5 if @keepalive
      socket
    end

    private def connect_tls(socket)
      socket.sync = true
      socket.read_buffering = false
      ctx = OpenSSL::SSL::Context::Client.new
      ctx.verify_mode = @verify_mode
      tls_socket = OpenSSL::SSL::Socket::Client.new(socket, ctx, sync_close: true, hostname: @host)
      tls_socket.sync = false
      tls_socket.read_buffering = true
      tls_socket.buffer_size = 16384
      tls_socket
    end

    private def connect_unix
      UNIXSocket.new(@host).tap do |socket|
        socket.sync = false
        socket.read_buffering = true
        socket.buffer_size = 16384
        socket.read_timeout = @keepalive * 1.5 if @keepalive
      end
    end
  end
end
