# TinyRedis

Production-quality Redis-inspired in-memory database and distributed cache written in Rust.

## Features

- Core key-value storage with strings, lists, sets, hashes, and sorted sets
- TTL expiration (lazy + active sampling)
- RESP-inspired protocol over async TCP (Tokio)
- Interactive CLI
- Transactions (`MULTI` / `EXEC` / `DISCARD` / `WATCH`)
- Pub/Sub
- Snapshot + AOF persistence with crash recovery
- Argon2 authentication
- Primary/replica replication
- Consistent-hash clustering primitives
- Memory limits with LRU/LFU eviction
- Prometheus metrics and HTTP health endpoint
- Docker Compose stack with Prometheus and Grafana

## Quick start

```bash
cargo run -p tinyredis-server
cargo run -p tinyredis-cli
```

In the CLI:

```text
127.0.0.1:6379> SET name Udit
OK
127.0.0.1:6379> GET name
"Udit"
```

## Workspace layout

```text
crates/
├── tinyredis-core/       # Storage engine and commands
├── tinyredis-protocol/   # RESP codec and parser
├── tinyredis-server/     # Async TCP server
└── tinyredis-cli/        # Interactive client
```

## Configuration

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `TINYREDIS_HOST` | `127.0.0.1` | Bind address |
| `TINYREDIS_PORT` | `6379` | TCP port |
| `TINYREDIS_DATA_DIR` | `./data` | Persistence directory |
| `TINYREDIS_MAX_MEMORY` | unlimited | Memory limit (bytes) |
| `TINYREDIS_PASSWORD` | none | Require AUTH |
| `TINYREDIS_LOG_LEVEL` | `info` | Log filter |

Health check HTTP endpoint: `http://<host>:<port+1>/health`

## Benchmarks (M18)

```bash
cargo bench -p tinyredis-core
cargo bench -p tinyredis-protocol
```

Reports are written under `target/criterion/`.

## Fuzzing (M17)

Proptest fuzz-style tests run with `cargo test --workspace`.

For libFuzzer targets (requires [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)):

```bash
cd fuzz
cargo fuzz run fuzz_resp_decoder -- -max_total_time=10
cargo fuzz run fuzz_command_parser -- -max_total_time=10
cargo fuzz run fuzz_snapshot_parser -- -max_total_time=10
cargo fuzz run fuzz_replication_parser -- -max_total_time=10
```

## Development

```bash
cargo fmt
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
cargo build --workspace --release
```

## Docker

```bash
docker compose up --build
```

## License

MIT
