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
# Run with default settings (UDP on :14550, Kafka on localhost:9092)
cargo run --release

# Or use a config file
cp config.example.toml config.toml
# Edit config.toml as needed
cargo run --release

# Or override with environment variables
MAVLINK_TO_KAFKA__MAVLINK__CONNECTION_STRING="tcpout:127.0.0.1:5760" cargo run --release
```

## Configuration

Configuration is layered (each layer overrides the previous):

1. **Defaults** — sensible built-in values
2. **Config file** — resolved in order:
   - `MAVLINK_TO_KAFKA_CONFIG` env var (explicit path override)
   - `./config.toml` (current working directory)
   - `/etc/mavlink-to-kafka/config.toml` (system / Docker path)
3. **Environment variables** — prefixed with `MAVLINK_TO_KAFKA__` (double underscore separates nesting)

See [`config.example.toml`](config.example.toml) for all available options.

### Environment Variable Examples

```sh
export MAVLINK_TO_KAFKA__MAVLINK__CONNECTION_STRING="udpin:0.0.0.0:14550"
export MAVLINK_TO_KAFKA__KAFKA__BROKERS="kafka1:9092,kafka2:9092"
export MAVLINK_TO_KAFKA__KAFKA__TOPIC_PREFIX="uav"
export MAVLINK_TO_KAFKA__LOGGING__LEVEL="debug"
```

## Docker

### Build the image

```sh
docker build -t mavlink-to-kafka .
```

### Run standalone

```sh
docker run --rm \
  -e MAVLINK_TO_KAFKA__KAFKA__BROKERS="host.docker.internal:9092" \
  -p 14550:14550/udp \
  mavlink-to-kafka
```

### Run with Docker Compose

The included `docker-compose.yml` starts Kafka (KRaft mode, no ZooKeeper) alongside the bridge:

```sh
docker compose up
```

The compose file mounts `./config.toml` into the container and overrides the Kafka broker address via environment variable so the bridge reaches Kafka over the Docker network.

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
