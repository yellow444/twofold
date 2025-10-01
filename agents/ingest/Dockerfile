# syntax=docker/dockerfile:1.6

ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE} AS base

ARG COMPONENT_NAME=agent-ingest
ENV COMPONENT_NAME=${COMPONENT_NAME} \
    ARTIFACTS_DIR=/artifacts \
    CACHE_DIR=/cache \
    PIP_CACHE_DIR=/cache/pip

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libmagic1 \
        poppler-utils \
        ghostscript \
        libxml2 \
        libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . .

RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir .

RUN chmod +x ./scripts/*.sh

FROM base AS lint
CMD ["./scripts/lint.sh"]

FROM base AS test
CMD ["./scripts/test.sh"]

FROM base AS build
CMD ["./scripts/build.sh"]

FROM base AS publish
CMD ["./scripts/publish.sh"]

FROM base AS runtime
ENTRYPOINT ["python", "-m", "app.cli"]
CMD ["--help"]
