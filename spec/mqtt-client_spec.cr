require "./spec_helper"

describe MQTT::Client do
  it "can publish" do
    done = Channel(Nil).new
    mqtt = MQTT::Client.new("localhost", 1883)
    mqtt.start
    i = 0
    mqtt.on_message do |msg|
      msg.topic.should eq "foo"
      msg.body.should eq "bar".to_slice
      done.send nil if (i += 1) == 2
    end
    mqtt.subscribe("foo", 1)
    mqtt.publish("foo", "bar", 1)
    mqtt.publish("foo", "bar", 1)
    done.receive
  end
end
