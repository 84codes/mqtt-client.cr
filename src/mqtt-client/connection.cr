require "./message"

module MQTT
  class Client
    class Error < Exception; end
    class UnexpectedPacket < Error; end
    class ConnectError < Error; end
    class InvalidProtocolVersion < ConnectError; end
    class IdentifierReject < ConnectError; end
    class NotAuthorized < ConnectError; end
    class ServerUnavailable < ConnectError; end
    class BadCredentials < ConnectError; end
    class InvalidResponse < ConnectError
      def initialize(@response_code : UInt8)
      end
    end

    class Connection
      @on_message : Proc(Message, Nil)?
      @packet_id = 0u16
      @acks = Channel(UInt16).new

      def initialize(@socket : IO, @client_id = "", @clean_session = true,
                     @user : String? = nil, @password : String? = nil,
                     @will : Message? = nil, @keepalive : UInt16 = 60u16)
        send_connect(@socket)
        expect_connack(@socket)
        spawn ping_loop, name: "mqtt-client ping_loop"
      end

      private def send_connect(socket) : Nil
        socket.write_byte 0b00010000u8 # type + flags

        length = 10
        length += 2 + @client_id.bytesize
        if u = @user
          length += 2 + u.bytesize
        end
        if p = @password
          length += 2 + p.bytesize
        end

        encode_length(socket, length)

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
        send_string(socket, @user.not_nil!) if @user
        send_string(socket, @password.not_nil!) if @password

        socket.flush
      end

      private def expect_connack(socket)
        b = socket.read_byte || raise IO::EOFError.new
        type = b >> 4          # upper 4 bits
        flags = b & 0b00001111 # lower 4 bits
        pktlen = decode_length(socket)

        case type
        when 2 then connack(socket, flags, pktlen)
        else        raise UnexpectedPacket.new
        end
      end

      # http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718021
      def read_loop
        socket = @socket
        loop do
          b = socket.read_byte || break
          type = b >> 4          # upper 4 bits
          flags = b & 0b00001111 # lower 4 bits
          pktlen = decode_length(socket)

          case type
          when 2     then connack(socket, flags, pktlen)
          when 3     then publish(socket, flags, pktlen)
          when 4     then puback(socket, flags, pktlen)
          when 5     then pubrec(socket, flags, pktlen)
          when 6     then pubrel(socket, flags, pktlen)
          when 7     then pubcomp(socket, flags, pktlen)
          when 9     then suback(socket, flags, pktlen)
          when 11    then unsuback(socket, flags, pktlen)
          when 13    then pingresp(socket, flags, pktlen)
          when 0, 15 then raise "forbidden packet type, reserved"
          else            raise "invalid packet type for server to send"
          end
        rescue IO::EOFError
          break
        end
      end

      private def connack(socket, flags, pktlen)
        session_present = (socket.read_byte || raise IO::EOFError.new) == 1u8
        return_code = socket.read_byte || raise IO::EOFError.new
        case return_code
        when 0u8 then return session_present
        when 1u8 then raise InvalidProtocolVersion.new
        when 2u8 then raise IdentifierReject.new
        when 3u8 then raise ServerUnavailable.new
        when 4u8 then raise BadCredentials.new
        when 5u8 then raise NotAuthorized.new
        else          raise InvalidResponse.new(return_code)
        end
      end

      private def ping_loop
        keepalive = @keepalive
        return if keepalive.zero?
        loop do
          sleep keepalive
          send_pingreq(@socket)
        end
      end

      private def pingresp(socket, flags, pktlen)
      end

      def on_message=(blk : Proc(Message, Nil)?)
        @on_message = blk
      end

      def on_message(&blk : Message -> Nil)
        @on_message = blk
      end

      def subscribe(*topics : Tuple(String, Int))
        id = send_subscribe(@socket, *topics)
        wait_for_id(id)
      end

      private def send_subscribe(socket, *topics : Tuple(String, Int))
        socket.write_byte 0b10000010u8

        length = 2 + topics.sum { |t, _| 2 + t.bytesize + 1 }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic, qos|
          send_string(socket, topic)
          socket.write_byte qos.to_u8
        end
        socket.flush
        id
      end

      def unsubscribe(*topics : String)
        id = send_unsubscribe(@socket, topics)
        wait_for_id(id)
      end

      private def wait_for_id(id)
        loop do
          ack_id = @acks.receive
          break if ack_id == id
          @acks.send id # if unexpected id, put it back on the channel
        end
      end

      private def send_unsubscribe(socket, topics)
        socket.write_byte 0b10000010u8

        length = 2 + topics.sum { |t| 2 + t.bytesize }
        encode_length(socket, length)

        id = send_next_packet_id(socket)
        topics.each do |topic|
          send_string(socket, topic)
        end
        id
      end

      def publish(msg : Message)
        publish(msg.topic, msg.body, msg.qos, msg.retain)
      end

      def publish(topic : String, body : String, qos : Int = 0u8, retain = false)
        publish(topic, body.to_slice, qos, retain)
      end

      def publish(topic : String, body : Bytes, qos : Int = 0u8, retain = false)
        if id = send_publish(@socket, topic, body, qos.to_u8, retain)
          wait_for_id(id)
        end
      end

      def send_publish(socket, topic, body, qos, retain = false, dup = false) : UInt16?
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

      private def publish(socket, flags, pktlen)
        dup = flags.bit(3) == 1
        qos = (flags & 0b00000110) >> 1
        retain = flags.bit(0) == 1
        topic = read_string(socket)
        header_len = 2 + topic.bytesize
        packet_id = read_int(socket) if qos > 0
        header_len += 2 if packet_id

        body = Bytes.new(pktlen - header_len)
        socket.read_fully(body)

        case qos
        when 1 then send_puback(socket, packet_id.not_nil!)
        when 2 then send_pubrec(socket, packet_id.not_nil!)
        end

        message = Message.new(topic, body, qos, retain, dup)
        @on_message.try &.call(message)
      end

      private def send_pingreq(socket)
        socket.write_byte 0b11000000u8
        socket.write_byte 0u8
        socket.flush
      end

      private def puback(socket, flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(socket)
        @acks.send packet_id
      end

      private def pubrec(socket, flags, pktlen)
        flags.zero? || raise "invalid pubrec flags"
        pktlen == 2 || raise "invalid pubrec length"

        packet_id = read_int(socket)
        send_pubrel(socket, packet_id)
      end

      private def pubrel(socket, flags, pktlen)
        flags.zero? || raise "invalid pubrel flags"
        pktlen == 2 || raise "invalid pubrel length"

        packet_id = read_int(socket)
        @acks.send packet_id
      end

      private def pubcomp(socket, flags, pktlen)
        flags.zero? || raise "invalid pubcomp flags"
        pktlen == 2 || raise "invalid pubcomp length"

        packet_id = read_int(socket)
        @acks.send packet_id
      end

      private def suback(socket, flags, pktlen)
        flags.zero? || raise "invalid suback flags"
        packet_id = read_int(socket)

        qos_len = pktlen - 2
        qoses = Array(UInt8).new(qos_len) do
          socket.read_byte || raise IO::EOFError.new
        end
        @acks.send packet_id
      end

      private def unsuback(socket, flags, pktlen)
        flags.zero? || raise "invalid puback flags"
        pktlen == 2 || raise "invalid puback length"

        packet_id = read_int(socket)
        @acks.send packet_id
      end

      private def send_puback(socket, packet_id)
        socket.write_byte 0b01000000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
      end

      private def send_pubrec(socket, packet_id)
        socket.write_byte 0b01100010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
      end

      private def send_pubrel(socket, packet_id)
        socket.write_byte 0b01110010 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
      end

      private def send_pubcomp(socket, packet_id)
        socket.write_byte 0b01110000 # type + flags
        socket.write_byte 2u8        # length
        socket.write_bytes packet_id, IO::ByteFormat::NetworkEndian
        socket.flush
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
    end
  end
end
