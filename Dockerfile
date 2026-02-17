FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y cmake build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

# Build real source
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/mavlink-to-kafka /usr/local/bin/mavlink-to-kafka

ENTRYPOINT ["mavlink-to-kafka"]
