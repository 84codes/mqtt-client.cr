require "./message"
require "./errors"

module MQTT
  class Client
    struct SocketOptions
      property buffer_size, recv_buffer_size, send_buffer_size

      def initialize(@buffer_size : Int32 = 1024,
                     @recv_buffer_size : Int32 = 512,
                     @send_buffer_size : Int32 = 256)
      end
    end

    class Connection
      Log = ::Log.for(self)

      @acks = Channel(UInt16).new
      @on_message : Proc(ReceivedMessage, Nil)?
      @messages = Channel(ReceivedMessage).new(16)
      @last_packet_received = Time.monotonic
      @last_packet_sent = Time.monotonic
      @packet_id = 0u16
      @keepalive = 60u16
      getter? connected = false

      def self.new(host : String, port = 1883, tls = false, client_id = "", clean_session = true,
                   user : String? = nil, password : String? = nil, will : Message? = nil,
                   keepalive : Int = 60u16, autoack = true, sock_opts = SocketOptions.new,
                   on_message : Proc(ReceivedMessage, Nil)? = nil)
        Log.debug { "creating connection to #{host}:#{port}" }
        if tls
          socket = connect_tls(connect_tcp(host, port, keepalive, sock_opts), OpenSSL::SSL::VerifyMode::PEER, host)
          Connection.new(socket, client_id, clean_session, user, password, will, keepalive.to_u16, autoack, on_message)
        else
          socket = connect_tcp(host, port, keepalive, sock_opts)
          Connection.new(socket, client_id, clean_session, user, password, will, keepalive.to_u16, autoack, on_message)
        end
      end

      def initialize(@socket : IO, @client_id = "", @clean_session = true,
                     @user : String? = nil, @password : String? = nil,
                     @will : Message? = nil, @keepalive : UInt16 = 60u16,
                     @autoack = false, @on_message : Proc(ReceivedMessage, Nil)? = nil)
        send_connect
        expect_connack
        @connected = true
        spawn read_loop, name: "mqtt-client read_loop"
        spawn message_loop, name: "mqtt-client message_loop"
      end

      def disconnect
        send_disconnect(@socket)
        Log.trace { "disconnected" }
        close
      end

      def close
        @connected = false
        @socket.close rescue nil
        @messages.close
        @acks.close
      end

      private def send_connect : Nil
        Log.trace { "sending connect" }
        socket = @socket
        socket.write_byte 0b00010000u8 # type + flags

        encode_length(socket, connect_length)

        send_string(socket, "MQTT")
        socket.write_byte 0x04 # protocol version 3.1.1

        flags = 0u8
        flags |= (1u8 << 1) if @clean_session
        if w = @will
          flags |= (1u8 << 2)
          flags |= (w.qos << 3)
          flags |= (1u8 << 5) if w.retain
        end
        flags |= (1u8 << 6) if @password
        flags |= (1u8 << 7) if @user
        socket.write_byte flags

        socket.write_bytes (@keepalive || 0).to_u16, IO::ByteFormat::NetworkEndian

        send_string(socket, @client_id)
        if w = @will
          send_string(socket, w.topic)
          socket.write_bytes w.body.bytesize.to_u16, IO::ByteFormat::NetworkEndian
          socket.write w.body
        end
        if user = @user
          send_string(socket, user)
        end
        if password = @password
          send_string(socket, password)
        end

        Log.trace { "sent connect" }
        socket.flush
        update_last_packet_sent
      end

      private def connect_length : Int32
        length = 10
        length += 2 + @client_id.bytesize
        if u = @user
          length += 2 + u.bytesize
        end
        if p = @password
          length += 2 + p.bytesize
        end
        if w = @will
          length += 2 + w.topic.bytesize + 2 + w.body.bytesize
        end
        length
      end

      private def expect_connack
        Log.trace { "waiting for connack" }
        socket = @socket
        b = socket.read_byte || raise IO::EOFError.new
        type = b >> 4          # upper 4 bits
        flags = b & 0b00001111 # lower 4 bits
        pktlen = decode_length(socket)

        case type
        when 2 then connack(flags, pktlen)
        else        raise UnexpectedPacket.new
        end
        Log.trace { "received connack" }
      rescue ex : IO::TimeoutError
        raise TimeoutError.new("Connect timeout", cause: ex)
      end

      def message_loop(messages = @messages)
        loop do
          message = messages.receive? || break
          if on_message = @on_message
            on_message.call(message)
          end
          message.ack if @autoack
        end
      end

      # http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718021
      private def read_loop(socket = @socket) # ameba:disable Metrics/CyclomaticComplexity
        loop do
          b = socket.read_byte || break
          type = b >> 4          # upper 4 bits
          flags = b & 0b00001111 # lower 4 bits
          pktlen = decode_length(socket)

          Log.trace { "got type #{type}" }
          case type
          when 2     then connack(flags, pktlen)
          when 3     then publish(flags, pktlen)
          when 4     then puback(flags, pktlen)
          when 5     then pubrec(flags, pktlen)
          when 6     then pubrel(flags, pktlen)
          when 7     then pubcomp(flags, pktlen)
          when 9     then suback(flags, pktlen)
          when 11    then unsuback(flags, pktlen)
          when 13    then pingresp(flags, pktlen)
          when 0, 15 then raise "forbidden packet type, reserved"
          else            raise "invalid packet type for server to send"
          end

          maybe_send_ping
        rescue ex : IO::TimeoutError
          try_send_ping(ex)
        rescue ex : IO::Error
          Log.trace { "io:error #{ex}\n\t#{ex.backtrace.join("\n\t")}" } if @connected
          break
        end
      rescue ex
        raise ex if @connected
      ensure
        close
      end

      private def maybe_send_ping
        return unless @keepalive.positive?

        now = Time.monotonic
        @last_packet_received = now
        if (now - @last_packet_sent).total_seconds > @keepalive * 0.9
          send_pingreq
        end
      end

      private def try_send_ping(ex)
        raise ex unless @keepalive.positive?

        now = Time.monotonic
        ping_diff = now - @last_packet_received
        if ping_diff.total_seconds > @keepalive * 1.5
          raise TimeoutError.new("No ping response from server in #{ping_diff}", cause: ex)
        else
          send_pingreq
        end
      end

      private def connack(flags, pktlen)
        socket = @socket
        session_present = (socket.read_byte || raise IO::EOFError.new) == 1u8
        return_code = socket.read_byte || raise IO::EOFError.new
        case return_code
        when 0u8
          Log.trace { "connected, session_present #{session_present}" }
          session_present
        when 1u8 then raise InvalidProtocolVersion.new
        when 2u8 then raise IdentifierReject.new
        when 3u8 then raise ServerUnavailable.new
        when 4u8 then raise BadCredentials.new
        when 5u8 then raise NotAuthorized.new
        else          raise InvalidResponse.new(return_code)
        end
      end

      private def pingresp(flags, pktlen)
        flags.zero? || raise "invalid pingresp flags"
        pktlen.zero? || raise "invalid pingresp length"
      end

      def ping
        send_pingreq
      end

      def puback(packet_id : UInt16)
        send_puback(packet_id)
      end

      def pubrec(packet_id : UInt16)
        send_pubrec(packet_id)
      end

      def on_message=(blk : Proc(ReceivedMessage, Nil)?)
        @on_message = blk
      end

      def on_message(&blk : Proc(ReceivedMessage, Nil))
        @on_message = blk
      end

      def subscribe(topic : String, qos : UInt8 = 0u8)
        subscribe({topic, qos})
      end

      def subscribe(*topics : Tuple(String, UInt8))
        subscribe(topics.to_a)
      end

      def subscribe(topics : Enumerable(Tuple(String, UInt8)))
        id = send_subscribe(@socket, topics)
        wait_for_id(id)
      end

      private def send_subscribe(socket, topics : Enumerable(Tuple(String, UInt8)))
        socket.write_byte 0b10000010u8

        length = 2 + topics.sum { |t, _| 2 + t.bytesize + 1 }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic, qos|
          send_string(socket, topic)
          socket.write_byte qos.to_u8
        end
        socket.flush
        update_last_packet_sent
        id
      end

      private def send_disconnect(socket) : Nil
        socket.write_byte 0b11100000u8
        socket.write_byte 0u8
        socket.flush
        update_last_packet_sent
      end

      def unsubscribe(*topics : String)
        id = send_unsubscribe(@socket, topics)
        wait_for_id(id)
      end

      private def wait_for_id(id : UInt16)
        acks = @acks
        loop do
          ack_id = acks.receive
          break if ack_id == id
          acks.send ack_id # if unexpected id, put it back on the channel
        end
      end

      private def send_unsubscribe(socket, topics)
        socket.write_byte 0b10100010u8

        length = 2 + topics.sum { |t| 2 + t.bytesize }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic|
          send_string(socket, topic)
        end
        socket.flush
        update_last_packet_sent
        id
      end

      def publish(msg : Message)
        publish(msg.topic, msg.body, msg.qos, msg.retain)
      end

      def publish(topic : String, body, qos : Int = 0u8, retain = false, dup = false)
        if id = send_publish(@socket, topic, body.to_slice, qos.to_u8, retain, dup)
          wait_for_id(id)
        end
      end

      def send_publish(socket : IO, topic : String, body : Slice, qos : UInt8, retain : Bool, dup : Bool) : UInt16?
        raise ArgumentError.new("Invalid QoS") unless 0 <= qos <= 2

        header = 0b00110000u8
        header |= (1u8 << 3) if dup
        header |= (qos << 1)
        header |= (1u8 << 0) if retain
        socket.write_byte header # type + flags

        length = 2 + topic.bytesize + body.bytesize
        length += 2 if qos > 0
        encode_length(socket, length)

        send_string(socket, topic)
        id = send_next_packet_id(socket) if qos > 0
        socket.write body
        socket.flush
        update_last_packet_sent
        id
      end

      private def send_next_packet_id(socket) : UInt16
        id = next_packet_id
        socket.write_bytes id, IO::ByteFormat::NetworkEndian
        id
      end

      private def next_packet_id : UInt16
        id = @packet_id &+ 1u16 # let it wrap around on overflow
        id = 1u16 if id.zero?
        @packet_id = id
      end

      private def publish(flags, pktlen)
        socket = @socket
        dup = flags.bit(3) == 1
        qos = (flags & 0b00000110) >> 1
        retain = flags.bit(0) == 1
        topic = read_string(socket)
        header_len = 2 + topic.bytesize
        packet_id = 0u16
        if qos > 0
          packet_id = read_int(socket)
          header_len += 2
        end
        body = Bytes.new(pktlen - header_len)
        socket.read_fully(body)

        @messages.send(ReceivedMessage.new(self, packet_id, topic, body, qos, retain, dup))
      end

      private def send_pingreq
        socket = @socket
        socket.write_byte 0b11000000u8
        socket.write_byte 0u8
        socket.flush
        update_last_packet_sent
      end

      private def puback(flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def pubrec(flags, pktlen)
        flags.zero? || raise "invalid pubrec flags"
        pktlen == 2 || raise "invalid pubrec length"

        packet_id = read_int(@socket)
        send_pubrel(packet_id)
      end

      private def pubrel(flags, pktlen)
        flags.zero? || raise "invalid pubrel flags"
        pktlen == 2 || raise "invalid pubrel length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def pubcomp(flags, pktlen)
        flags.zero? || raise "invalid pubcomp flags"
        pktlen == 2 || raise "invalid pubcomp length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def suback(flags, pktlen)
        flags.zero? || raise "invalid suback flags"
        socket = @socket
        packet_id = read_int(socket)

        qos_len = pktlen - 2
        Array(UInt8).new(qos_len) do
          socket.read_byte || raise IO::EOFError.new
        end
        @acks.send packet_id
      end

      private def unsuback(flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(@socket)
        @acks.send packet_id
      end

      private def send_puback(packet_id)
        socket = @socket
        socket.write_byte 0b01000000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubrec(packet_id)
        socket = @socket
        socket.write_byte 0b01100010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubrel(packet_id)
        socket = @socket
        socket.write_byte 0b01110010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_pubcomp(packet_id)
        socket = @socket
        socket.write_byte 0b01110000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
        update_last_packet_sent
      end

      private def send_string(socket : IO, str : String)
        socket.write_bytes str.bytesize.to_u16, IO::ByteFormat::NetworkEndian
        socket.write str.to_slice
      end

      private def read_string(socket)
        len = read_int(socket)
        socket.read_string(len)
      end

      private def read_int(socket)
        socket.read_bytes UInt16, IO::ByteFormat::NetworkEndian
      end

      private def update_last_packet_sent
        @last_packet_sent = Time.monotonic if @keepalive.positive?
      end

      private def decode_length(socket)
        multiplier = 1
        value = 0
        loop do
          b = socket.read_byte || raise IO::EOFError.new
          value = (b & 127) * multiplier
          multiplier *= 128
          raise "invalid packet length" if multiplier > 128*128*128
          break if b & 128 == 0
        end
        value
      end

      private def encode_length(socket, length)
        loop do
          b = (length % 128).to_u8
          length = length // 128
          b = b | 128 if length > 0
          socket.write_byte b
          break if length <= 0
        end
      end

      private def self.connect_tcp(host, port, keepalive, sock_opts : SocketOptions)
        socket = TCPSocket.new(host, port, connect_timeout: 30)
        socket.keepalive = true
        socket.tcp_nodelay = false
        socket.tcp_keepalive_idle = 60
        socket.tcp_keepalive_count = 3
        socket.tcp_keepalive_interval = 10
        socket.sync = false
        socket.read_buffering = true
        socket.buffer_size = sock_opts.buffer_size if sock_opts.buffer_size.positive?
        socket.recv_buffer_size = sock_opts.recv_buffer_size if sock_opts.recv_buffer_size.positive?
        socket.send_buffer_size = sock_opts.send_buffer_size if sock_opts.send_buffer_size.positive?
        socket.read_timeout = keepalive
        socket
      end

      private def self.connect_tls(socket, verify_mode : LibSSL::VerifyMode, host : String)
        ctx = OpenSSL::SSL::Context::Client.new
        ctx.verify_mode = verify_mode
        connect_tls(socket, ctx, host)
      end

      private def self.connect_tls(socket, ctx, host)
        socket.sync = true
        socket.read_buffering = false
        tls_socket = OpenSSL::SSL::Socket::Client.new(socket, ctx, sync_close: true, hostname: host)
        tls_socket.sync = false
        tls_socket.read_buffering = true
        tls_socket.buffer_size = 16384
        tls_socket
      end
    end
  end
end
