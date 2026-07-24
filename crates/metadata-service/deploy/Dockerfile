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
RUN cargo chef cook --release --recipe-path recipe.json -p metadata-service

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY tools ./tools
RUN cargo build --release -p metadata-service

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/metadata-service /app/metadata-service

ENV BIND=0.0.0.0:8000
EXPOSE 8000
ENTRYPOINT ["/app/metadata-service"]
