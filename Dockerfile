# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required by repo-squirrel
RUN apt-get update && \
    apt-get install -y --no-install-recommends git cloc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source
COPY . /app

# Install Python dependencies (currently Flask only)
RUN pip install --no-cache-dir flask && \
    chmod +x /app/docker-entrypoint.sh && \
    mkdir -p /app/repos /app/stats

ENV HOST=0.0.0.0 \
    PORT=5001 \
    READ_ONLY=false

EXPOSE 5001
VOLUME ["/app/repos", "/app/stats", "/app/configuration"]

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD []
