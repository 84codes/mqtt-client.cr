require "./spec_helper"

describe MQTT::Client do
  it "can publish" do
    done = Channel(Nil).new
    mqtt = MQTT::Client.new("localhost", 1883)
    mqtt.start
    mqtt.on_message do |msg|
      msg.topic.should eq "foo"
      msg.body.should eq "bar".to_slice
      done.send nil
    end
    mqtt.subscribe("foo", 1)
    mqtt.publish("foo", "bar", 1)
    mqtt.publish("foo", "bar", 1)
    mqtt.close
    done.receive
    done.receive
  end

  it "can ping" do
    mqtt = MQTT::Client.new("localhost", 1883)
    mqtt.connect.ping.should eq nil
  end

  it "can keepalive" do
    mqtt = MQTT::Client.new("localhost", 1883, keepalive: 1u16)
    mqtt.start
    sleep 1.5
    mqtt.close
  end
end
