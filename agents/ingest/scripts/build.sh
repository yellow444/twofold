#!/usr/bin/env sh
set -euo pipefail

: "${COMPONENT_NAME:=agent-ingest}"
: "${ARTIFACTS_DIR:=/artifacts}"
: "${CACHE_DIR:=/cache}"

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_DIR="$(dirname -- "${SCRIPT_DIR}")"
PIP_CACHE_DIR="${CACHE_DIR}/pip"

mkdir -p "${ARTIFACTS_DIR}" "${PIP_CACHE_DIR}"

cd "${PROJECT_DIR}"

python -m pip install --cache-dir "${PIP_CACHE_DIR}" --upgrade pip >/dev/null
python -m pip install --cache-dir "${PIP_CACHE_DIR}" --upgrade . build >/dev/null

python -m build --outdir "${ARTIFACTS_DIR}"
