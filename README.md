# mavlink-to-kafka

A Rust application that bridges MAVLink messages and Apache Kafka. It reads MAVLink messages and publishes each one as JSON to Kafka topics named `mavlink.<MESSAGE_NAME>`. Optionally, it can consume command messages from a Kafka topic and send them as MAVLink messages over the same connection.

## Architecture

```
Kafka StreamConsumer.recv() (async) → deserialize JSON → spawn_blocking(conn.send()) → MAVLink
MAVLink conn.recv() (spawn_blocking) → mpsc → serialize JSON → FutureProducer.send() → Kafka
                                CancellationToken (Ctrl+C)
```

The `mavlink` crate's `recv()` and `send()` are blocking, so they run via `spawn_blocking`. The MAVLink connection is shared between the reader and command sender via `Arc` — this is safe because `MavConnection::send()` takes `&self`.

## Building

```sh
cargo build --release
```

## Usage

```sh
# UDP listener (default)
cargo run -- -m "udpin:0.0.0.0:14550" -b "localhost:9092"

# TCP connection
cargo run -- -m "tcpout:127.0.0.1:5760" -b "localhost:9092"

# Serial
cargo run -- -m "serial:/dev/ttyUSB0:57600" -b "localhost:9092"

# With command consumer enabled
cargo run -- -m "udpin:0.0.0.0:14550" -b "localhost:9092" --command-topic "mavlink.commands"
```

### CLI Options

| Flag | Description |
|------|-------------|
| `-c, --config <PATH>` | Path to configuration file |
| `-m, --mavlink-connection <STRING>` | MAVLink connection string |
| `-b, --brokers <STRING>` | Kafka broker addresses |
| `-t, --topic-prefix <STRING>` | Topic prefix (default: `mavlink`) |
| `--command-topic <STRING>` | Kafka command topic (enables command consumer) |
| `--consumer-group <STRING>` | Consumer group ID for command consumer |
| `-l, --log-level <LEVEL>` | Log level: trace, debug, info, warn, error |

## Configuration

Configuration is layered (each layer overrides the previous):

1. **Defaults** — sensible built-in values
2. **TOML file** — `config.toml` (or path passed via `--config`)
3. **Environment variables** — prefixed with `MAVLINK_TO_KAFKA__` (double underscore separates nesting)
4. **CLI arguments** — highest priority

See [`config.example.toml`](config.example.toml) for all available options.

### Environment Variable Examples

```sh
export MAVLINK_TO_KAFKA__MAVLINK__CONNECTION_STRING="udpin:0.0.0.0:14550"
export MAVLINK_TO_KAFKA__KAFKA__BROKERS="kafka1:9092,kafka2:9092"
export MAVLINK_TO_KAFKA__KAFKA__TOPIC_PREFIX="uav"
export MAVLINK_TO_KAFKA__KAFKA__COMMANDS__ENABLED="true"
export MAVLINK_TO_KAFKA__KAFKA__COMMANDS__COMMAND_TOPIC="mavlink.commands"
export MAVLINK_TO_KAFKA__LOGGING__LEVEL="debug"
```

## Kafka Output

Messages are published to topics named `<prefix>.<MESSAGE_NAME>`, e.g.:
- `mavlink.HEARTBEAT`
- `mavlink.GPS_RAW_INT`
- `mavlink.SYS_STATUS`

The partition key is the MAVLink `system_id`, preserving per-vehicle message ordering.

### JSON Format

```json
{
  "header": {
    "system_id": 1,
    "component_id": 1,
    "sequence": 42
  },
  "message": {
    "type": "HEARTBEAT",
    "custom_mode": 0,
    "mavtype": { "type": "MAV_TYPE_QUADROTOR" },
    "autopilot": { "type": "MAV_AUTOPILOT_ARDUPILOTMEGA" },
    "base_mode": { "bits": 0 },
    "system_status": { "type": "MAV_STATE_ACTIVE" },
    "mavlink_version": 3
  }
}
```

## Commands (Kafka → MAVLink)

When enabled, the bridge consumes JSON command messages from a Kafka topic and sends them as MAVLink messages. Enable via config (`kafka.commands.enabled = true`) or CLI (`--command-topic`).

### Command JSON Format

Commands use the same JSON format as outbound messages:

```json
{
  "header": {
    "system_id": 255,
    "component_id": 190
  },
  "message": {
    "type": "HEARTBEAT",
    "custom_mode": 0,
    "mavtype": { "type": "MAV_TYPE_GCS" },
    "autopilot": { "type": "MAV_AUTOPILOT_INVALID" },
    "base_mode": { "bits": 0 },
    "system_status": { "type": "MAV_STATE_ACTIVE" },
    "mavlink_version": 3
  }
}
```

The `sequence` field in the header is optional (defaults to 0) — the MAVLink connection manages sequencing internally.

### Publishing Commands

```sh
# Using kcat/kafkacat
echo '{"header":{"system_id":255,"component_id":190},"message":{"type":"HEARTBEAT","custom_mode":0,"mavtype":{"type":"MAV_TYPE_GCS"},"autopilot":{"type":"MAV_AUTOPILOT_INVALID"},"base_mode":{"bits":0},"system_status":{"type":"MAV_STATE_ACTIVE"},"mavlink_version":3}}' | kcat -P -b localhost:9092 -t mavlink.commands
```

## MAVLink Dialect

Uses the `ardupilotmega` dialect, which is a superset of `common`, `icarous`, and `uavionix`.

## Testing

```sh
cargo test
```
