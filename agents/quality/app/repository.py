"""Database persistence helpers for the quality validation pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from psycopg import Connection, connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .checks import CheckStatus


@dataclass(slots=True)
class QualityReportEntry:
    """Normalized payload describing a single quality check outcome."""

    check_name: str
    severity: CheckStatus
    payload: dict[str, Any]
    region_id: int | None = None


@dataclass(slots=True)
class QualityRepository:
    """Provide high-level helpers for persisting quality results."""

    database_url: str

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        """Yield a transactional psycopg connection."""

        conn = connect(self.database_url)
        try:
            yield conn
            conn.commit()
        except Exception:  # pragma: no cover - defensive rollback
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_dataset_version_id(self, conn: Connection, version_name: str) -> int:
        """Return the primary key for ``dataset_version`` matching ``version_name``."""

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

    def replace_quality_reports(
        self,
        conn: Connection,
        dataset_version_id: int,
        entries: Iterable[QualityReportEntry],
    ) -> None:
        """Replace existing rows in ``quality_report`` for the dataset version."""

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM quality_report
                 WHERE dataset_version_id = %s
                """,
                (dataset_version_id,),
            )
            for entry in entries:
                cur.execute(
                    """
                    INSERT INTO quality_report (
                        dataset_version_id,
                        region_id,
                        check_name,
                        severity,
                        details
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        dataset_version_id,
                        entry.region_id,
                        entry.check_name,
                        entry.severity.value,
                        Jsonb(entry.payload),
                    ),
                )

    def _ensure_quality_columns(self, conn: Connection) -> None:
        """Ensure auxiliary quality-related columns exist (idempotent)."""

        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE dataset_version
                ADD COLUMN IF NOT EXISTS quality_warn_count INTEGER NOT NULL DEFAULT 0
                """
            )
            cur.execute(
                """
                ALTER TABLE dataset_version
                ADD COLUMN IF NOT EXISTS quality_fail_count INTEGER NOT NULL DEFAULT 0
                """
            )

    def update_dataset_version(
        self,
        conn: Connection,
        dataset_version_id: int,
        *,
        status: str,
        warn_count: int,
        fail_count: int,
    ) -> None:
        """Persist aggregate status flags back to ``dataset_version``."""

        self._ensure_quality_columns(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dataset_version
                   SET status = %s,
                       validated_at = NOW(),
                       quality_warn_count = %s,
                       quality_fail_count = %s
                 WHERE id = %s
                """,
                (status, warn_count, fail_count, dataset_version_id),
            )


__all__ = [
    "QualityReportEntry",
    "QualityRepository",
]
