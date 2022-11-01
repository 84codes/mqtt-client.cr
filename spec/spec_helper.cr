require "spec"
require "../src/mqtt-client"
require "mqtt-protocol"

class SpecServer
  def initialize(@server : TCPServer)
  end

  def address
    @server.local_address
  end

  def with_client(&blk : MQTT::Protocol::IO -> Nil)
    spawn(name: "with_client") do
      client_socket = @server.accept?
      raise "accept? returned nil" if client_socket.nil?
      blk.call MQTT::Protocol::IO.new(client_socket)
      client_socket.close
    end
  end
end

def with_server_socket
  TCPServer.open("127.0.0.1", 0, reuse_port: false) do |tcp|
    yield SpecServer.new(tcp)
  end
end
