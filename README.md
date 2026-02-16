# mavlink-to-kafka

A Rust application that reads MAVLink messages from a single source (TCP, UDP, or serial) and publishes each message as JSON to Kafka topics named `mavlink.<MESSAGE_NAME>`.

## Architecture

```
spawn_blocking(mavlink recv) --[mpsc channel]--> async task(serialize + FutureProducer.send) --> Kafka
                                    ^
                            CancellationToken (Ctrl+C)
```

The `mavlink` crate's `recv()` is blocking, so it runs on a dedicated `spawn_blocking` task that sends messages over a `tokio::sync::mpsc` channel. The main async task consumes from the channel, serializes to JSON, and publishes via `rdkafka::FutureProducer`. Graceful shutdown is coordinated with `CancellationToken`.

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
```

### CLI Options

| Flag | Description |
|------|-------------|
| `-c, --config <PATH>` | Path to configuration file |
| `-m, --mavlink-connection <STRING>` | MAVLink connection string |
| `-b, --brokers <STRING>` | Kafka broker addresses |
| `-t, --topic-prefix <STRING>` | Topic prefix (default: `mavlink`) |
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

## MAVLink Dialect

Uses the `ardupilotmega` dialect, which is a superset of `common`, `icarous`, and `uavionix`.

## Testing

```sh
cargo test
```
