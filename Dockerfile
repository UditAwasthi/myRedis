FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release -p tinyredis-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 tinyredis
WORKDIR /data
COPY --from=builder /app/target/release/tinyredis-server /usr/local/bin/tinyredis-server
USER tinyredis
ENV TINYREDIS_HOST=0.0.0.0 \
    TINYREDIS_PORT=6379 \
    TINYREDIS_DATA_DIR=/data
EXPOSE 6379
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD bash -c 'echo -e "PING\r\n" | nc -w 2 localhost 6379 | grep -q PONG || exit 1'
ENTRYPOINT ["tinyredis-server"]
