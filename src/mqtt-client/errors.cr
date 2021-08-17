module MQTT
    class Error < Exception; end

    class TimeoutError < Error; end

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
end
