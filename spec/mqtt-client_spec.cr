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
    mqtt.subscribe("foo", 2)
    mqtt.publish("foo", "bar", 2)
    done.receive
  end
end
