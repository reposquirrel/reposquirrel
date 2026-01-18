# syntax=docker/dockerfile:1
FROM python:3.11-slim

ARG TOKEI_VERSION=14.0.0

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required by repo-squirrel
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl ca-certificates cargo && \
    rm -rf /var/lib/apt/lists/*

# Install tokei with JSON output support
RUN cargo install tokei --locked --features all --version ${TOKEI_VERSION} && \
    mv /root/.cargo/bin/tokei /usr/local/bin/tokei && \
    rm -rf /root/.cargo

WORKDIR /app

# Copy source
COPY . /app

# Install Python dependencies (currently Flask only)
RUN pip install --no-cache-dir flask && \
    chmod +x /app/docker-entrypoint.sh && \
    mkdir -p /app/repos /app/stats

ENV HOST=0.0.0.0 \
    PORT=5001 \
    READ_ONLY=false \
    TOKEI_BIN=/usr/local/bin/tokei

EXPOSE 5001
VOLUME ["/app/repos", "/app/stats", "/app/configuration"]

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD []
