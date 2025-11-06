FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential cargo rustc && \
    rm -rf /var/lib/apt/lists/*

COPY ./scripts/last_block_backfill/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./scripts/last_block_backfill/last_block_back_fill.py .

CMD ["python", "-u", "last_block_back_fill.py"]

