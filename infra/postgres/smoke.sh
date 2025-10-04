#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/infra/docker-compose.yml"
SERVICE_NAME="postgres"
DB_NAME="${POSTGRES_DB:-bpla}"
DB_USER="${POSTGRES_USER:-bpla_user}"
SQL_FILE="${REPO_ROOT}/infra/postgres/smoke.sql"
TMP_FILE="/tmp/smoke.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "[smoke] SQL script not found: ${SQL_FILE}" >&2
  exit 1
fi

echo "[smoke] Running smoke checks from ${SQL_FILE}"
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" \
  bash -c "cat > '${TMP_FILE}'" < "${SQL_FILE}"
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" \
  psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" -f "${TMP_FILE}"
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" rm -f "${TMP_FILE}"
echo "[smoke] Smoke checks completed."
