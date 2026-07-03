# Builds the `reshuffle-sim` tool into a runnable image whose entrypoint replays the
# reshuffle simulation against an external Postgres (the DB URL is injected via the
# DATABASE_URL env var; the scheduler config and the input assignment file are supplied
# by the entrypoint script — see entrypoint.sh).
#
# Build context is the repository root, e.g.:
#   docker build -f tools/reshuffle_sim/deploy/Dockerfile -t subsquid/reshuffle-sim .
#
# cargo-chef layer caching: https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.94-slim-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools ./tools
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      protobuf-compiler pkg-config libssl-dev build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
# Compile dependencies first so they stay cached across source-only changes. reshuffle-sim
# pulls in network-scheduler with the `mvcc-chunks` + `pg-testkit` features via its own
# Cargo.toml, so no extra --features flag is needed here.
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
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
