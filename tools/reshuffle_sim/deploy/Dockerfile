# Builds the reshuffle-sim image; its entrypoint (see entrypoint.sh) replays the sim against an
# external Postgres ($DATABASE_URL), with config + input supplied by the entrypoint.
#
# Build context is the repo root, e.g.:
#   docker build -f tools/reshuffle_sim/deploy/Dockerfile -t subsquid/reshuffle-sim .
#
# cargo-chef layer caching: https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.94-slim-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY tools ./tools
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      protobuf-compiler libprotobuf-dev pkg-config libssl-dev build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
# Cook only reshuffle-sim's dependency closure (-p), not every workspace member's. Stays cached
# across source-only changes.
RUN cargo chef cook --release --recipe-path recipe.json -p reshuffle-sim

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY tools ./tools
RUN cargo build --release -p reshuffle-sim

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 tar \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/reshuffle-sim /app/reshuffle-sim
COPY tools/reshuffle_sim/deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
