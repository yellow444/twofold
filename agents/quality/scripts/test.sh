#!/usr/bin/env bash
set -euo pipefail

: "${ARTIFACTS_DIR:=/artifacts}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

mkdir -p "${ARTIFACTS_DIR}"
cd "${PROJECT_DIR}"
export PYTHONPATH="$(pwd)/../..:${PYTHONPATH:-}"

python -m pytest --junitxml "${ARTIFACTS_DIR}/pytest.xml"
