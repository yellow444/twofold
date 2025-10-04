"""Database helpers for the geo agent geocoding pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


@dataclass(slots=True)
class FlightCandidate:
    """Joined representation of a flight from ``flights_norm`` and ``flights_raw``."""

    dataset_version_id: int
    flight_uid: str
    observed_at: datetime
    latitude: float | None
    longitude: float | None
    payload_flight_id: str | None


@dataclass(slots=True)
class FlightGeometry:
    """Geocoding result ready for persistence."""

    flight_uid: str
    region_id: int
    latitude: float
    longitude: float
    observed_at: datetime


@dataclass(slots=True)
class FlightQualityIssueRecord:
    """Representation of an unresolved flight location issue."""

    flight_uid: str
    severity: str
    details: dict[str, Any]


class GeoRepository:
    """Read/write helpers for geocoding persistent state."""

    def fetch_dataset_version_id(self, conn: Connection, version_name: str) -> int:
        """Return the primary key for ``dataset_version`` identified by ``version_name``."""

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id
                  FROM dataset_version
                 WHERE version_name = %s
                """,
                (version_name,),
            )
            row = cur.fetchone()
            if not row:
                msg = f"dataset_version '{version_name}' not found"
                raise ValueError(msg)
            return int(row["id"])

    def load_flight_candidates(self, conn: Connection, dataset_version_id: int) -> list[FlightCandidate]:
        """Return candidate flights for geocoding for the dataset version."""

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT fn.dataset_version_id,
                       fn.flight_uid,
                       fn.departure_time AS observed_at,
                       fr.payload->>'latitude' AS latitude,
                       fr.payload->>'longitude' AS longitude,
                       fr.payload->>'flight_id' AS payload_flight_id
                  FROM flights_norm AS fn
                  JOIN flights_raw AS fr
                    ON fr.dataset_version_id = fn.dataset_version_id
                   AND fr.flight_external_id = fn.flight_uid
                 WHERE fn.dataset_version_id = %s
                ORDER BY fn.flight_uid
                """,
                (dataset_version_id,),
            )
            rows = cur.fetchall()

        candidates: list[FlightCandidate] = []
        for row in rows:
            candidates.append(
                FlightCandidate(
                    dataset_version_id=int(row["dataset_version_id"]),
                    flight_uid=str(row["flight_uid"]),
                    observed_at=row["observed_at"],
                    latitude=self._to_float(row["latitude"]),
                    longitude=self._to_float(row["longitude"]),
                    payload_flight_id=row["payload_flight_id"],
                )
            )
        return candidates

    def resolve_region(self, conn: Connection, latitude: float, longitude: float) -> int | None:
        """Return the region identifier containing the provided coordinate."""

        with conn.cursor() as cur:
            cur.execute(
                """
                WITH point AS (
                    SELECT ST_SetSRID(ST_Point(%s, %s), 4326) AS geom
                )
                SELECT r.id
                  FROM regions AS r
                  CROSS JOIN point
                 WHERE ST_Intersects(r.boundary, point.geom)
                 ORDER BY ST_Distance(r.boundary, point.geom), r.id
                 LIMIT 1
                """,
                (longitude, latitude),
            )
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0])

    def clear_flights_geo(self, conn: Connection, dataset_version_id: int) -> None:
        """Remove previously persisted geocoding rows for the dataset version."""

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM flights_geo
                 WHERE dataset_version_id = %s
                """,
                (dataset_version_id,),
            )

    def insert_flight_geometries(
        self, conn: Connection, dataset_version_id: int, records: Iterable[FlightGeometry]
    ) -> None:
        """Persist geocoded points into ``flights_geo``."""

        params = [
            (
                dataset_version_id,
                record.region_id,
                record.flight_uid,
                record.longitude,
                record.latitude,
                record.observed_at,
            )
            for record in records
        ]
        if not params:
            return

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO flights_geo (
                    dataset_version_id,
                    region_id,
                    flight_uid,
                    location,
                    observed_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    ST_SetSRID(ST_Point(%s, %s), 4326),
                    %s
                )
                ON CONFLICT (dataset_version_id, flight_uid, observed_at)
                DO UPDATE SET
                    region_id = EXCLUDED.region_id,
                    location = EXCLUDED.location
                """,
                params,
            )

    def clear_flight_regions(self, conn: Connection, dataset_version_id: int) -> None:
        """Reset ``region_id`` for normalized flights in the dataset version."""

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE flights_norm
                   SET region_id = NULL
                 WHERE dataset_version_id = %s
                """,
                (dataset_version_id,),
            )

    def update_flight_regions(
        self, conn: Connection, dataset_version_id: int, records: Iterable[FlightGeometry]
    ) -> None:
        """Update ``flights_norm.region_id`` for geocoded flights."""

        params = [
            (record.region_id, dataset_version_id, record.flight_uid) for record in records
        ]
        if not params:
            return

        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE flights_norm
                   SET region_id = %s
                 WHERE dataset_version_id = %s
                   AND flight_uid = %s
                """,
                params,
            )

    def replace_quality_issues_for_check(
        self,
        conn: Connection,
        dataset_version_id: int,
        check_name: str,
        issues: Iterable[FlightQualityIssueRecord],
    ) -> None:
        """Replace quality issues for a dataset version and check name."""

        issue_params = [
            (
                dataset_version_id,
                issue.flight_uid,
                check_name,
                issue.severity,
                Jsonb(issue.details),
            )
            for issue in issues
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM flight_quality_issues
                 WHERE dataset_version_id = %s
                   AND check_name = %s
                """,
                (dataset_version_id, check_name),
            )
            if not issue_params:
                return
            cur.executemany(
                """
                INSERT INTO flight_quality_issues (
                    dataset_version_id,
                    flight_uid,
                    check_name,
                    severity,
                    details
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                issue_params,
            )

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None
