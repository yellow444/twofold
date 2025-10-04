"""Database persistence utilities for the ingest pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterator, Optional

import polars as pl
import pyarrow as pa
from psycopg import Connection, connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import PostgresSettings


@dataclass(slots=True)
class DatabaseRepository:
    """Helper providing high-level persistence operations."""

    settings: PostgresSettings

    def _connection_kwargs(self) -> Dict[str, object]:
        kwargs: Dict[str, object] = {
            "host": self.settings.host,
            "port": self.settings.port,
            "dbname": self.settings.database,
            "user": self.settings.user,
        }
        if self.settings.password:
            kwargs["password"] = self.settings.password
        if self.settings.sslmode:
            kwargs["sslmode"] = self.settings.sslmode
        return kwargs

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        """Context manager returning a psycopg connection with transaction handling."""

        conn = connect(**self._connection_kwargs())
        try:
            yield conn
            conn.commit()
        except Exception:  # pragma: no cover - defensive rollback
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_artifacts_column(self, conn: Connection) -> None:
        """Ensure auxiliary metadata columns exist (idempotent)."""

        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE dataset_version
                ADD COLUMN IF NOT EXISTS artifacts JSONB
                """
            )

    def create_dataset_version(
        self,
        conn: Connection,
        *,
        version_name: str,
        year: int,
        source_uri: Optional[str],
        status: str = "new",
    ) -> int:
        """Insert a new dataset_version row and return its id."""

        if year is None:
            raise ValueError("year must be provided for dataset_version")
        self._ensure_artifacts_column(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO dataset_version (version_name, year, source_uri, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (version_name, year, source_uri, status),
            )
            row = cur.fetchone()
            if not row:  # pragma: no cover - should not happen
                raise RuntimeError("Failed to insert dataset_version")
            return int(row["id"])

    def copy_flights_raw(
        self,
        conn: Connection,
        *,
        dataset_version_id: int,
        table: pa.Table,
        only_valid: bool = True,
    ) -> int:
        """Bulk-copy normalized records into flights_raw staging table."""

        df = pl.from_arrow(table) if not isinstance(table, pl.DataFrame) else table
        if df.is_empty():
            return 0
        if only_valid and "superseded" in df.columns:
            df = df.filter(~pl.col("superseded"))
        if df.is_empty():
            return 0

        count = 0
        with conn.cursor() as cur:
            with cur.copy(
                "COPY flights_raw (dataset_version_id, flight_external_id, event_date, payload) FROM STDIN"
            ) as copy:
                for row in df.iter_rows(named=True):
                    flight_id = row.get("flight_id")
                    if flight_id is None:
                        continue
                    start: datetime | None = row.get("start_time_utc")
                    if start is None:
                        continue
                    payload = _serialize_payload(row)
                    copy.write_row(
                        (
                            dataset_version_id,
                            flight_id,
                            start.date(),
                            Jsonb(payload),
                        )
                    )
                    count += 1
        return count

    def upsert_flights_norm(
        self,
        conn: Connection,
        *,
        dataset_version_id: int,
        table: pa.Table,
    ) -> int:
        """Bulk insert normalized records into flights_norm table."""

        df = pl.from_arrow(table) if not isinstance(table, pl.DataFrame) else table
        if df.is_empty():
            return 0
        if "superseded" in df.columns:
            df = df.filter(~pl.col("superseded"))
        if df.is_empty():
            return 0

        count = 0
        with conn.cursor() as cur:
            with cur.copy(
                "COPY flights_norm (dataset_version_id, region_id, flight_uid, departure_time, arrival_time, duration_minutes) "
                "FROM STDIN"
            ) as copy:
                for row in df.iter_rows(named=True):
                    start: datetime | None = row.get("start_time_utc")
                    if start is None:
                        continue
                    copy.write_row(
                        (
                            dataset_version_id,
                            None,
                            row.get("flight_id"),
                            start,
                            row.get("end_time_utc"),
                            row.get("duration_minutes"),
                        )
                    )
                    count += 1
        return count

    def mark_ingested(
        self,
        conn: Connection,
        *,
        dataset_version_id: int,
        checksum: str,
        artifacts: Dict[str, str],
    ) -> None:
        """Mark dataset_version as ingested updating checksum and artifact references."""

        self._ensure_artifacts_column(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dataset_version
                   SET status = 'ingested',
                       ingested_at = NOW(),
                       checksum = %s,
                       artifacts = %s
                 WHERE id = %s
                """,
                (checksum, Jsonb(artifacts), dataset_version_id),
            )


def _serialize_payload(row: dict[str, object]) -> Dict[str, object]:
    """Convert row values to JSON-serialisable types."""

    payload: Dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        elif isinstance(value, date):
            payload[key] = value.isoformat()
        else:
            payload[key] = value
    return payload
