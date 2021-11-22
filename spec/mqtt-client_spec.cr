require "./spec_helper"

describe MQTT::Client do
  it "can publish" do
    done = Channel(Nil).new
    mqtt = MQTT::Client.new(MQTT_HOST, 1883, client_id: "can publish")
    mqtt.on_message do |msg|
      msg.topic.should eq "foo"
      msg.body.should eq "bar".to_slice
      msg.ack
      done.send nil
    end
    mqtt.subscribe("foo", 1)
    mqtt.publish("foo", "bar", 1)
    mqtt.publish("foo", "bar", 1)
    mqtt.unsubscribe("foo")
    mqtt.publish("foo", "bar", 1)
    done.receive
    done.receive
  ensure
    mqtt.try &.close
  end

  it "can ping" do
    mqtt = MQTT::Client.new(MQTT_HOST, 1883, client_id: "can ping")
    mqtt.ping
    mqtt.@connection.@last_packet_received.should be_close Time.monotonic, 1.second
  end

  it "can keepalive" do
    mqtt = MQTT::Client.new(MQTT_HOST, 1883, keepalive: 1u16, client_id: "can keepalive")
    sleep 1.5
  ensure
    mqtt.try &.close
  end
end
