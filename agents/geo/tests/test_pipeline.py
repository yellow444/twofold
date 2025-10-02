from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.geo.app.pipeline import GeoPipeline


@pytest.fixture()
def schema_setup(postgresql):
    dsn = postgresql.info.dsn
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dataset_version (
                    id BIGSERIAL PRIMARY KEY,
                    version_name TEXT NOT NULL UNIQUE,
                    year SMALLINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'new'
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS regions (
                    id BIGSERIAL PRIMARY KEY,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    boundary GEOMETRY(MultiPolygon, 4326) NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS flights_raw (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_version_id BIGINT NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
                    region_id BIGINT REFERENCES regions(id),
                    flight_external_id TEXT NOT NULL,
                    event_date DATE NOT NULL,
                    payload JSONB,
                    UNIQUE(dataset_version_id, flight_external_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS flights_norm (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_version_id BIGINT NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
                    region_id BIGINT REFERENCES regions(id),
                    flight_uid TEXT NOT NULL,
                    departure_time TIMESTAMPTZ NOT NULL,
                    UNIQUE(dataset_version_id, flight_uid)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS flights_geo (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_version_id BIGINT NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
                    region_id BIGINT REFERENCES regions(id),
                    flight_uid TEXT NOT NULL,
                    location GEOMETRY(Point, 4326) NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL,
                    UNIQUE(dataset_version_id, flight_uid, observed_at)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS flight_quality_issues (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_version_id BIGINT NOT NULL REFERENCES dataset_version(id) ON DELETE CASCADE,
                    flight_uid TEXT NOT NULL,
                    check_name TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
    return dsn


def _seed_data(dsn: str) -> tuple[int, int]:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE flights_geo, flight_quality_issues, flights_norm, flights_raw RESTART IDENTITY CASCADE"
            )
            cur.execute("TRUNCATE TABLE regions RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE TABLE dataset_version RESTART IDENTITY CASCADE")
            cur.execute(
                "INSERT INTO dataset_version (version_name, year, status) VALUES (%s, %s, %s) RETURNING id",
                ("2024.01", 2024, "new"),
            )
            dataset_version_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO regions (code, name, boundary)
                VALUES (%s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
                RETURNING id
                """,
                ("R1", "Region 1", "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
            )
            region_a_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO regions (code, name, boundary)
                VALUES (%s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
                RETURNING id
                """,
                ("R2", "Region 2", "POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))"),
            )
            _ = cur.fetchone()

            observed_at = datetime(2024, 1, 5, 12, 30, tzinfo=timezone.utc)

            flights = [
                (
                    "FLIGHT-INSIDE",
                    {"latitude": 5.5, "longitude": 5.5, "flight_id": "INSIDE"},
                ),
                (
                    "FLIGHT-BOUNDARY",
                    {"latitude": 10.0, "longitude": 5.0, "flight_id": "BOUNDARY"},
                ),
                (
                    "FLIGHT-ZERO",
                    {"latitude": 0.0, "longitude": 0.0, "flight_id": "ZERO"},
                ),
            ]

            for flight_uid, payload in flights:
                cur.execute(
                    """
                    INSERT INTO flights_raw (
                        dataset_version_id,
                        flight_external_id,
                        event_date,
                        payload
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (dataset_version_id, flight_uid, observed_at.date(), json.dumps(payload)),
                )
                cur.execute(
                    """
                    INSERT INTO flights_norm (
                        dataset_version_id,
                        flight_uid,
                        departure_time
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (dataset_version_id, flight_uid, observed_at),
                )

        conn.commit()

    return dataset_version_id, region_a_id


@pytest.mark.skipif(shutil.which("pg_ctl") is None, reason="PostgreSQL server binaries not available")
def test_pipeline_geocodes_and_records_quality(postgresql, schema_setup) -> None:
    dsn = schema_setup
    dataset_version_id, region_a_id = _seed_data(dsn)

    pipeline = GeoPipeline()
    with psycopg.connect(dsn) as conn:
        summary = pipeline.run_for_version(conn, "2024.01")

    assert summary.dataset_version_id == dataset_version_id
    assert summary.total_candidates == 3
    assert summary.resolved == 2
    assert summary.unresolved == 1

    expected_observed_at = datetime(2024, 1, 5, 12, 30, tzinfo=timezone.utc)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT flight_uid, region_id, ST_X(location), ST_Y(location), observed_at
                  FROM flights_geo
                 ORDER BY flight_uid
                """
            )
            geo_rows = cur.fetchall()
            cur.execute(
                """
                SELECT flight_uid, region_id
                  FROM flights_norm
                 WHERE dataset_version_id = %s
                 ORDER BY flight_uid
                """,
                (dataset_version_id,),
            )
            norm_rows = cur.fetchall()
            cur.execute(
                """
                SELECT flight_uid, check_name, severity, details->>'reason'
                  FROM flight_quality_issues
                 WHERE dataset_version_id = %s
                 ORDER BY flight_uid
                """,
                (dataset_version_id,),
            )
            issue_rows = cur.fetchall()

    assert geo_rows == [
        ("FLIGHT-BOUNDARY", region_a_id, pytest.approx(10.0), pytest.approx(5.0), expected_observed_at),
        ("FLIGHT-INSIDE", region_a_id, pytest.approx(5.5), pytest.approx(5.5), expected_observed_at),
    ]

    assert norm_rows == [
        ("FLIGHT-BOUNDARY", region_a_id),
        ("FLIGHT-INSIDE", region_a_id),
        ("FLIGHT-ZERO", None),
    ]

    assert issue_rows == [
        ("FLIGHT-ZERO", "geo.unresolved_location", "fail", "zero_coordinates"),
    ]
