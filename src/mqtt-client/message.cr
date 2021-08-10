module MQTT
  class Client
    record Message, topic : String, body : Bytes, qos : UInt8, retain = false, dup = false
  end
end
