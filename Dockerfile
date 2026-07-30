# See https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef for explanation
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.94-slim-bookworm AS chef
WORKDIR /app


FROM chef AS planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates
COPY src ./src
COPY tools ./tools
RUN cargo chef prepare --recipe-path recipe.json --bin network-scheduler


FROM chef AS builder
RUN apt-get update && apt-get install protobuf-compiler pkg-config libssl-dev build-essential  -y

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p network-scheduler

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates
COPY src ./src
COPY tools ./tools
# `-p`, not a workspace build: Cargo unifies features across members, so building everything
# gave the scheduler `mvcc-chunks` and a docker client because reshuffle-sim asks for them.
# reshuffle-sim ships from tools/reshuffle_sim/deploy/Dockerfile.
RUN cargo build --release -p network-scheduler


FROM chef AS scheduler
RUN apt-get update && apt-get install -y net-tools
COPY --from=builder /app/target/release/network-scheduler /app/network-scheduler

CMD ["/app/network-scheduler"]
