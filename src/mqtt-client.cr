require "socket"
require "openssl"
require "./mqtt-client/connection"

module MQTT
  class Client
    def initialize(@host : String, @port = 1883, @tls = false, @client_id = "",
                   @clean_session = true, @user : String? = nil,
                   @password : String? = nil, @will : Message? = nil,
                   @keepalive : Int = 60u16, @sock_opts = SocketOptions.new)
      @verify_mode = OpenSSL::SSL::VerifyMode::PEER
      @reconnect_interval = 1
      @connection = connect
    end

    @connection : Connection
    @lock = Mutex.new

    def publish(topic : String, body : Bytes, qos : Int = 0u8, retain = false)
      publish(Message.new(topic, body, qos.to_u8, retain))
    end

    def publish(topic : String, body : String, qos : Int = 0u8, retain = false)
      publish(Message.new(topic, body.to_slice, qos.to_u8, retain))
    end

    def publish(*messages : Message)
      with_connection do |connection|
        messages.each do |message|
          connection.publish message
        end
      end
    end

    def subscribe(topic : String, qos : Int = 0u8)
      subscribe({topic, qos})
    end

    def subscribe(*topics : Tuple(String, Int))
      raise ArgumentError.new("No on_message handler set") unless @on_message
      with_connection &.subscribe(*topics)
    end

    def on_message(&blk : Message -> Nil)
      @on_message = blk
      with_connection &.on_message = blk
    end

    def unsubscribe(*topics : String)
      with_connection &.unsubscribe(*topics)
    end

    def ping
      with_connection &.ping
    end

    def disconnect
      with_connection &.disconnect
    end

    def close
      with_connection &.close
    end

    private def with_connection
      @lock.synchronize do
        @connection = reconnect unless @connection.connected?
        yield @connection
      end
    end

    private def reconnect
      loop do
        connection = connect
        connection.on_message = @on_message
        return connection
      rescue ex
        STDERR.puts "MQTT-Client reconnect error: #{ex.message}"
        sleep @reconnect_interval
      end
    end

    private def connect
      Connection.new(@host, @port, @tls, @client_id, @clean_session, @user, @password, @will, @keepalive.to_u16, @sock_opts)
    end
  end
end
