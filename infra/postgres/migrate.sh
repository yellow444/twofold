#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIGRATIONS_DIR="${REPO_ROOT}/infra/postgres/migrations"
COMPOSE_FILE="${REPO_ROOT}/infra/docker-compose.yml"
SERVICE_NAME="postgres"
DB_NAME="${POSTGRES_DB:-bpla}"
DB_USER="${POSTGRES_USER:-bpla_user}"
TMP_DIR="/tmp/migrations"

if [[ ! -d "${MIGRATIONS_DIR}" ]]; then
  echo "[migrate] Migrations directory not found: ${MIGRATIONS_DIR}" >&2
  exit 1
fi

mapfile -t migrations < <(find "${MIGRATIONS_DIR}" -maxdepth 1 -type f -name '*.sql' -print | sort)

if [[ ${#migrations[@]} -eq 0 ]]; then
  echo "[migrate] No migrations to apply."
  exit 0
fi

echo "[migrate] Applying ${#migrations[@]} migration(s) in alphabetical order..."
for migration in "${migrations[@]}"; do
  filename="$(basename "${migration}")"
  echo "[migrate] → ${filename}"
  docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" \
    bash -c "mkdir -p '${TMP_DIR}' && cat > '${TMP_DIR}/${filename}'" < "${migration}"
  docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" \
    psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d "${DB_NAME}" -f "${TMP_DIR}/${filename}"
  docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE_NAME}" rm -f "${TMP_DIR}/${filename}"
  echo "[migrate] ✔ ${filename} applied"
  echo
done

echo "[migrate] All migrations applied successfully."
