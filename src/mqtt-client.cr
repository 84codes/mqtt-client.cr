require "socket"
require "openssl"
require "./mqtt-client/connection"

module MQTT
  class Client
    def initialize(@host : String, @port = 1883, @tls = false, @client_id = "", @clean_session = true, @user : String? = nil, @password : String? = nil, @will : Message? = nil, @keepalive : Int = 60u16)
      @verify_mode = OpenSSL::SSL::VerifyMode::PEER
    end

    @closed = false
    @conn_chan = Channel(Connection).new

    def start
      @closed = false
      spawn connect_loop, name: "MQTT Client connect_loop"
    end

    def close
      with_connection do |_|
        @closed = true
      end
    end

    private def connect_loop
      connection = connect
      connection.on_message = @on_message
      loop do
        break if @closed
        @conn_chan.send connection
        connection = @conn_chan.receive
        break if @closed
        next if connection.connected?
        puts "MQTT reconnecting in 1s"
        sleep 1
        connection = connect
        connection.on_message = @on_message
      end
    ensure
      @conn_chan.close
      connection.try &.close
    end

    def connect
      if @tls
        socket = connect_tls(connect_tcp)
        Connection.new(socket, @client_id, @clean_session, @user, @password, @will, @keepalive.to_u16)
      else
        socket = connect_tcp
        Connection.new(socket, @client_id, @clean_session, @user, @password, @will, @keepalive.to_u16)
      end
    end

    def disconnect
      @closed = true
    end

    def publish(*messages : Message)
      with_connection do |connection|
        messages.each do |message|
          connection.publish message
        end
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

    def subscribe(topic : String, qos : Int = 0u8)
      subscribe({topic, qos})
    end

    def subscribe(*topics : Tuple(String, Int))
      raise ArgumentError.new("No on_message handler set") unless @on_message
      with_connection &.subscribe(*topics)
    end

    def unsubscribe(*topics : String)
      with_connection &.unsubscribe(*topics)
    end

    private def with_connection
      connection = @conn_chan.receive
      begin
        yield connection
      ensure
        @conn_chan.send connection
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
      ctx = OpenSSL::SSL::Context::Client.new
      ctx.verify_mode = @verify_mode
      connect_tls(socket, ctx)
    end

    private def connect_tls(socket, ctx)
      socket.sync = true
      socket.read_buffering = false
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
