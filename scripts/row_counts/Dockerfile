FROM python:3.14-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential python3-dev pkg-config ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /work
COPY scripts/row_counts/requirements.txt .

RUN python -m pip install -U pip wheel \
 && pip wheel --wheel-dir /wheels --prefer-binary -r requirements.txt

FROM python:3.14-slim
WORKDIR /app

COPY --from=builder /wheels /wheels
COPY scripts/row_counts/requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt
COPY scripts/row_counts/row_counts_fill.py .
CMD ["python", "-u", "row_counts_fill.py"]
