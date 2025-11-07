FROM python:3.11-slim

RUN apt-get update && apt-get install -y protobuf-compiler build-essential clang make cmake curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.local/bin:${PATH}"

# RUN curl -y --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
RUN curl -y -sSf https://sh.rustup.rs | sh

WORKDIR /app

COPY ./scripts/last_block_backfill/requirements.txt .

RUN uv pip compile requirements.txt -o uv.lock

RUN uv pip sync --system uv.lock

COPY ./scripts/last_block_backfill/last_block_back_fill.py .
CMD ["python", "-u", "last_block_back_fill.py"]

