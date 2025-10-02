-- 0002_quality_flags.sql
-- Track quality issues for specific flights

BEGIN;

CREATE TABLE IF NOT EXISTS flight_quality_issues (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    flight_uid          TEXT        NOT NULL,
    check_name          TEXT        NOT NULL,
    severity            TEXT        NOT NULL,
    details             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flight_quality_issues_search
    ON flight_quality_issues (dataset_version_id, flight_uid);

COMMIT;
