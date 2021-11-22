require "./connection"

module MQTT
  class Client
    record Message, topic : String, body : Bytes, qos : UInt8, retain = false, dup = false

    struct ReceivedMessage
      # The order is important to minimize `sizeof`
      @connection : Connection
      getter topic : String
      getter body : Bytes
      @packet_id : UInt16
      getter qos : UInt8
      getter retain : Bool
      getter dup : Bool

      def initialize(@connection : Connection, @packet_id : UInt16, @topic : String, @body : Bytes,
                     @qos : UInt8, @retain : Bool, @dup : Bool)
      end

      def ack
        case @qos
        when 1 then @connection.puback(@packet_id)
        when 2 then @connection.pubrec(@packet_id)
        end
      end
    end
  end
end
