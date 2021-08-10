# MQTT-Client

A MQTT client built in Crystal.

## Installation

1. Add the dependency to your `shard.yml`:

```yaml
dependencies:
  mqtt-client:
    github: 84codes/mqtt-client.cr
```

2. Run `shards install`

## Usage

The library has both a autoreconnecting highlevel API shown below:

```crystal
require "mqtt-client"

mqtt = MQTT::Client.new("localhost", 1883)

mqtt.on_message do |msg|
  puts "Got a message, on topic #{msg.topic}: #{String.new(msg.body)}"
end

mqtt.start

mqtt.subscribe("foo", qos: 1)

mqtt.publish("foo", "bar", qos: 1)

mqtt.close
```

And a basic API that is more true to the protocol but that requires you to handle reconnection manually.

```crystal
require "mqtt-client"

mqtt = MQTT::Client.new("localhost", 1883)

conn = mqtt.connect
mqtt.on_message do |msg|
  puts "Got a message, on topic #{msg.topic}: #{String.new(msg.body)}"
end
conn.publish MQTT::Client::Message.new("topic", "body".to_slice)

```
