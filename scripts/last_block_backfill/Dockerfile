# Use an official lightweight Python base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies (optional but recommended)
# Add any native libs you need for pip packages here (example: gcc, libpq-dev)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements.txt first for caching
COPY ./scripts/last_block_backfill/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your codebase into the container
COPY ./scripts/last_block_backfill/last_block_back_fill.py .

# Default command: run your Python script
CMD ["python", "-u", "last_block_back_fill.py"]

