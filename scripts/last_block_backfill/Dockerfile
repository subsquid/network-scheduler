FROM rust:1-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-venv python3-dev build-essential pkg-config \
      ca-certificates curl protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /w
COPY scripts/last_block_backfill/requirements.txt .

RUN python3 -m venv /w/venv
ENV PATH="/w/venv/bin:${PATH}"

RUN python3 -m pip install -U pip wheel \
 && pip wheel --wheel-dir /wheels --prefer-binary -r requirements.txt

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates zlib1g \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /wheels /wheels
COPY scripts/last_block_backfill/requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt

COPY scripts/last_block_backfill/last_block_back_fill.py .

CMD ["python", "-u", "last_block_back_fill.py"]

