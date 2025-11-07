FROM python:3.11-slim

RUN apt-get update && apt-get install -y zlib1g \
      ca-certificates protobuf-compiler build-essential clang make cmake curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

RUN which cargo && cargo --version && rustc --version

WORKDIR /app

COPY ./scripts/last_block_backfill/requirements.txt .

RUN uv pip compile requirements.txt -o uv.lock

RUN uv pip sync --system uv.lock

COPY ./scripts/last_block_backfill/last_block_back_fill.py .
CMD ["python", "-u", "last_block_back_fill.py"]

