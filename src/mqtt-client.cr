require "log"
require "socket"
require "openssl"
require "./mqtt-client/connection"

module MQTT
  class Client
    Log = ::Log.for(self)

    def self.new(*args, **kwargs, &)
      client = Client.new(*args, **kwnargs.merge(autoconnect: false))
      yield client
      client.connect
      client
    end

    def initialize(@host : String, @port = 1883, @tls = false, @client_id = "",
                   @clean_session = true, @user : String? = nil,
                   @password : String? = nil, @will : Message? = nil,
                   @keepalive : Int = 60u16, @autoack = true,
                   @sock_opts = SocketOptions.new, autoconnect = true)
      @subscriptions = Hash(String, UInt8).new
      @verify_mode = OpenSSL::SSL::VerifyMode::PEER
      @reconnect_interval = 1
      @connect = false
      Log.trace { "autoconnect = #{autoconnect}" }
      connect if autoconnect
    end

    @connection : Connection?
    @lock = Mutex.new

    def publish(topic : String, body, qos : Int = 0u8, retain = false)
      publish(Message.new(topic, body.to_slice, qos.to_u8, retain))
    end

    def publish(*messages : Message)
      with_connection do |conn|
        messages.each do |message|
          conn.publish message
        end
      end
    end

    def subscribe(topic : String, qos : Int = 0u8)
      subscribe({topic, qos.to_u8})
    end

    def subscribe(*topics : Tuple(String, UInt8))
      raise ArgumentError.new("No on_message handler set") unless @on_message
      with_connection do |conn|
        conn.subscribe(topics)
        topics.each { |t, q| @subscriptions[t] = q }
      end
    end

    def on_message(&blk : ReceivedMessage -> Nil)
      @on_message = blk
      with_connection? do |conn|
        conn.on_message = blk
      end
    end

    def unsubscribe(*topics : String)
      with_connection do |conn|
        conn.unsubscribe(*topics)
        topics.each { |t| @subscriptions.delete(t) }
      end
    end

    def ping
      with_connection &.ping
    end

    def disconnect
      with_connection &.disconnect
    end

    def close
      with_connection? &.close
    end

    def connect
      Log.trace { "connect @connect=#{@connect}" }
      return if @connect
      @connect = true
      @lock.synchronize { @connection = reconnect }
    end

    private def with_connection?
      @lock.synchronize do
        if conn = @connection
          yield conn
        end
      end
    end

    private def with_connection
      @lock.synchronize do
        raise "call connect first" unless @connect
        @connection = reconnect unless @connection.try &.connected?
        if conn = @connection
          yield conn
        else
          raise "BUG? @connection nil"
        end
      end
    end

    private def reconnect : Connection
      attempt = 0
      loop do
        attempt += 1
        Log.info { "connecting to #{@host}:#{@port}… Attempt #{attempt}" }
        connection = create_connection
        Log.info { "connected to #{@host}:#{@port}" }
        unless @subscriptions.empty?
          Log.info { "subscribing to topics: #{@subscriptions}" }
          connection.not_nil!.subscribe(@subscriptions.each)
        end
        return connection
      rescue ex
        Log.trace { "connect error\n\t#{ex.backtrace.join("\n\t")}" }
        sleep @reconnect_interval
      end
    end

    private def create_connection : Connection
      Connection.new(@host, @port, @tls, @client_id, @clean_session,
        @user, @password, @will, @keepalive.to_u16, @autoack,
        @sock_opts, @on_message)
    end
  end
end
