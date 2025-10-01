-- 0001_initial.sql
-- Creates base schema for UAV analytics platform

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE dataset_version (
    id              BIGSERIAL PRIMARY KEY,
    version_name    TEXT        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    source_uri      TEXT,
    status          TEXT        NOT NULL DEFAULT 'new',
    checksum        TEXT,
    ingested_at     TIMESTAMPTZ,
    validated_at    TIMESTAMPTZ,
    aggregated_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE regions (
    id              BIGSERIAL PRIMARY KEY,
    code            TEXT        NOT NULL UNIQUE,
    name            TEXT        NOT NULL,
    boundary        GEOMETRY(MultiPolygon, 4326) NOT NULL
);

CREATE TABLE flights_raw (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    region_id           BIGINT      REFERENCES regions(id),
    flight_external_id  TEXT        NOT NULL,
    event_date          DATE        NOT NULL,
    year                SMALLINT    GENERATED ALWAYS AS (EXTRACT(YEAR FROM event_date)::SMALLINT) STORED,
    month               SMALLINT    GENERATED ALWAYS AS (EXTRACT(MONTH FROM event_date)::SMALLINT) STORED,
    payload             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(dataset_version_id, flight_external_id)
);

CREATE TABLE flights_norm (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    region_id           BIGINT      REFERENCES regions(id),
    flight_uid          TEXT        NOT NULL,
    departure_time      TIMESTAMPTZ NOT NULL,
    arrival_time        TIMESTAMPTZ,
    duration_minutes    NUMERIC(10,2),
    year                SMALLINT    GENERATED ALWAYS AS (EXTRACT(YEAR FROM departure_time)::SMALLINT) STORED,
    month               SMALLINT    GENERATED ALWAYS AS (EXTRACT(MONTH FROM departure_time)::SMALLINT) STORED,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(dataset_version_id, flight_uid)
);

CREATE TABLE flights_geo (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    region_id           BIGINT      REFERENCES regions(id),
    flight_uid          TEXT        NOT NULL,
    location            GEOMETRY(Point, 4326) NOT NULL,
    observed_at         TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(dataset_version_id, flight_uid, observed_at)
);

CREATE TABLE aggregates_year (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    region_id           BIGINT      NOT NULL REFERENCES regions(id),
    year                SMALLINT    NOT NULL,
    flights_count       BIGINT      NOT NULL DEFAULT 0,
    duration_sum_min    NUMERIC(12,2) NOT NULL DEFAULT 0,
    duration_avg_min    NUMERIC(10,2),
    payload             JSONB,
    UNIQUE(dataset_version_id, region_id, year)
);

CREATE TABLE quality_report (
    id                  BIGSERIAL PRIMARY KEY,
    dataset_version_id  BIGINT      NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
    region_id           BIGINT      REFERENCES regions(id),
    check_name          TEXT        NOT NULL,
    severity            TEXT        NOT NULL,
    details             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- B-tree indexes for search keys
CREATE INDEX idx_flights_raw_search
    ON flights_raw (dataset_version_id, region_id, year, month);

CREATE INDEX idx_flights_norm_search
    ON flights_norm (dataset_version_id, region_id, year, month);

CREATE INDEX idx_flights_geo_search
    ON flights_geo (dataset_version_id, region_id);

CREATE INDEX idx_aggregates_year_search
    ON aggregates_year (dataset_version_id, region_id, year);

CREATE INDEX idx_quality_report_search
    ON quality_report (dataset_version_id, region_id);

-- Geospatial index
CREATE INDEX idx_flights_geo_location
    ON flights_geo USING GIST (location);

COMMIT;
