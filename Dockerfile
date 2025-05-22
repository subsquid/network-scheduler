# See https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef for explanation
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.87-slim-bookworm AS chef
WORKDIR /app


FROM chef AS planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
RUN apt-get update && apt-get install protobuf-compiler pkg-config libssl-dev build-essential  -y

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml .
COPY Cargo.lock .
COPY src ./src
RUN cargo build --release


FROM chef AS scheduler
RUN apt-get update && apt-get install -y net-tools
COPY --from=builder /app/target/release/network-scheduler /app/network-scheduler

CMD ["/app/network-scheduler"]
