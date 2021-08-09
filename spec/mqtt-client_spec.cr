require "./spec_helper"

describe MQTT::Client do
  it "can publish" do
    done = Channel(Nil).new
    mqtt = MQTT::Client.new("localhost", 1883)
    mqtt.on_message do |msg|
      msg.topic.should eq "foo"
      msg.body.should eq "bar".to_slice
      done.send nil
    end
    mqtt.subscribe("foo")
    mqtt.publish("foo", "bar".to_slice)
    done.receive
  end
end
