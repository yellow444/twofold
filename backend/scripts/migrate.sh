#!/usr/bin/env sh
set -eu

: "${COMPONENT_NAME:=backend}"
: "${ARTIFACTS_DIR:=/artifacts}"
: "${CACHE_DIR:=/cache}"

mkdir -p "${ARTIFACTS_DIR}" "${CACHE_DIR}"

artifact="${ARTIFACTS_DIR}/${COMPONENT_NAME}_migrate.log"
{
  echo "Running placeholder migrations for ${COMPONENT_NAME}."
  echo "timestamp=$(date -u +%FT%TZ)"
} > "${artifact}"

echo "Migration placeholder complete. Inspect ${artifact} for details."
