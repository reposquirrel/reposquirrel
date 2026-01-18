# syntax=docker/dockerfile:1
FROM python:3.11-slim

ARG TOKEI_VERSION=14.0.0

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required by repo-squirrel
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl ca-certificates tar && \
    rm -rf /var/lib/apt/lists/*

# Install tokei binary release (JSON support is included in release builds)
ARG TOKEI_ARCH=x86_64-unknown-linux-gnu
RUN set -euo pipefail && \
    curl -sSL https://github.com/XAMPPRocky/tokei/releases/download/v${TOKEI_VERSION}/tokei-${TOKEI_ARCH}.tar.gz -o /tmp/tokei.tar.gz && \
    tar -xzf /tmp/tokei.tar.gz -C /tmp && \
    install -m 0755 /tmp/tokei /usr/local/bin/tokei 2>/dev/null || install -m 0755 /tmp/tokei-*/tokei /usr/local/bin/tokei && \
    rm -rf /tmp/tokei.tar.gz /tmp/tokei /tmp/tokei-*

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
