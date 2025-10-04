# syntax=docker/dockerfile:1.6

ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE} AS base

ARG COMPONENT_NAME=agent-ingest
ENV COMPONENT_NAME=${COMPONENT_NAME} \
    ARTIFACTS_DIR=/artifacts \
    CACHE_DIR=/cache

WORKDIR /workspace

COPY scripts ./scripts
RUN chmod +x ./scripts/*.sh

COPY . .

FROM base AS lint
CMD ["./scripts/lint.sh"]

FROM base AS test
CMD ["./scripts/test.sh"]

FROM base AS build
CMD ["./scripts/build.sh"]

FROM base AS publish
CMD ["./scripts/publish.sh"]
