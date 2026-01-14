# syntax=docker/dockerfile:1
FROM python:3.11-slim

ARG OCLOC_VERSION=0.5.0
ARG OCLOC_ARCH=x86_64-unknown-linux-gnu

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install system dependencies required by repo-squirrel
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source
COPY . /app

# Install ocloc binary (prebuilt release)
RUN curl -fsSL "https://github.com/adhishthite/ocloc/releases/download/v${OCLOC_VERSION}/ocloc-${OCLOC_VERSION}-${OCLOC_ARCH}.tar.gz" -o /tmp/ocloc.tar.gz && \
    tar -xzf /tmp/ocloc.tar.gz -C /usr/local/bin ocloc && \
    chmod +x /usr/local/bin/ocloc && \
    rm /tmp/ocloc.tar.gz

# Install Python dependencies (currently Flask only)
RUN pip install --no-cache-dir flask && \
    chmod +x /app/docker-entrypoint.sh && \
    mkdir -p /app/repos /app/stats

ENV HOST=0.0.0.0 \
    PORT=5001 \
    READ_ONLY=false \
    OCLOC_BIN=/usr/local/bin/ocloc

EXPOSE 5001
VOLUME ["/app/repos", "/app/stats", "/app/configuration"]

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD []
