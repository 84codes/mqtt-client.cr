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

```crystal
require "mqtt-client"

mqtt = MQTT::Client.new("localhost", 1883)

mqtt.on_message do |msg|
  puts "Got a message, on topic #{msg.topic}: #{String.new(msg.body)}"
end

mqtt.subscribe("foo", qos: 1)

mqtt.publish("foo", "bar", qos: 1)

mqtt.close
```
