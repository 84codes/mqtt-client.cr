require "./spec_helper"

alias MP = MQTT::Protocol

describe MQTT::Client do
  it "can publish" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)

      # This "mocks" the server
      server.with_client do |client_io|
        MP::Packet.from_io(client_io)
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        sub = MP::Packet.from_io(client_io).as(MP::Subscribe)
        MP::SubAck.new([MP::SubAck::ReturnCode::QoS1], sub.packet_id).to_io(client_io)

        subscribed = true
        loop do
          packet = MP::Packet.from_io(client_io)
          case packet
          when MP::Publish
            pub = packet.as(MP::Publish)
            MP::PubAck.new(pub.packet_id.not_nil!).to_io(client_io)
            pub.to_io(client_io) if subscribed
          when MP::Unsubscribe
            unsub = packet.as(MP::Unsubscribe)
            subscribed = false
            MP::UnsubAck.new(unsub.packet_id).to_io(client_io)
          when MP::Disconnect
            break
          end
        end
        done.send(nil)
      end
      mqtt = MQTT::Client.new(server.address.address,
        port: server.address.port,
        client_id: "can publish")

      recieved_message_count = 0
      mqtt.on_message do |msg|
        msg.topic.should eq "foo"
        msg.body.should eq "bar".to_slice
        msg.ack
        recieved_message_count += 1
      end
      mqtt.subscribe("foo", 1)
      mqtt.publish("foo", "bar", 1)
      mqtt.publish("foo", "bar", 1)
      mqtt.unsubscribe("foo")
      mqtt.publish("foo", "bar", 1)
      mqtt.disconnect
      mqtt.close
      done.receive
      recieved_message_count.should eq 2
    end
  end

  it "can ping" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)

      server.with_client do |client_io|
        MP::Packet.from_io(client_io)
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        MP::Packet.from_io(client_io).as(MP::PingReq)
        MP::PingResp.new.to_io(client_io)

        done.send(nil)
      end

      mqtt = MQTT::Client.new(server.address.address, port: server.address.port, client_id: "can ping")
      mqtt.ping
      done.receive
      mqtt.@connection.not_nil!.@last_packet_received.should be_close Time.monotonic, 1.second
    end
  end

  it "can keepalive" do
    with_server_socket do |server|
      done = Channel(Nil).new(1)
      ping_recieved = false

      server.with_client do |client_io|
        connect = MP::Packet.from_io(client_io).as(MP::Connect)
        client_io.@io.as(Socket).read_timeout = connect.keepalive * 1.5
        MP::Connack.new(false, MP::Connack::ReturnCode::Accepted).to_io(client_io)

        MP::Packet.from_io(client_io).as(MP::PingReq)
        ping_recieved = true
      rescue IO::Error
        # nop
      ensure
        done.send(nil)
      end

      MQTT::Client.new(server.address.address, port: server.address.port, keepalive: 1u16, client_id: "can keepalive")
      done.receive
      ping_recieved.should be_true
    end
  end
end
