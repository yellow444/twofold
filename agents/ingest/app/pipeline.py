"""Core ingest pipeline implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pendulum
import polars as pl
import pyarrow as pa

from .config import AppSettings
from .formats import detect_format, load_records
from .lineage import LineageRecorder, ensure_polars as ensure_polars_df
from .logging import get_logger
from .normalization import normalize_records
from .repository import DatabaseRepository
from .storage import StorageClient


class IngestPipeline:
    _VERSION_FORMAT = "YYYYMMDD-HHmmss"

    """High-level pipeline orchestrating ingest steps."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings = settings or AppSettings()
        self.logger = get_logger("app.pipeline")

    def run(
        self,
        source: str,
        *,
        year: Optional[int] = None,
        fmt: Optional[str] = None,
        storage_path: Optional[Path] = None,
        dry_run: bool = False,
        dataset_version: Optional[str] = None,
    ) -> Tuple[pa.Table, dict[str, int]]:
        """Execute the ingest pipeline for the provided source."""

        resolved_version = dataset_version or self.settings.dataset_version
        target_path = storage_path or self.settings.dataset_root
        context = {
            "source": source,
            "year": year,
            "format": fmt,
            "storage_path": str(target_path) if target_path else None,
            "dry_run": dry_run,
            "dataset_version": resolved_version,
        }

        self.logger.info("Starting ingest pipeline", extra={"context": context})

        if dry_run:
            self.logger.info(
                "Dry run enabled, skipping dataset persistence.",
                extra={"context": context},
            )

        detected_format = detect_format(source, format_hint=fmt)
        load_result = load_records(source, format_hint=detected_format)
        table, counters = normalize_records(load_result)

        resolved_version = resolved_version or self._generate_version()
        raw_df = ensure_polars_df(getattr(load_result, "records", None))
        inferred_year = year or self._infer_year(table) or self.settings.default_year
        if inferred_year is None and not dry_run:
            raise ValueError("Year must be provided or derivable for persistence")

        storage_prefix = str(storage_path).strip("/") if storage_path else None

        self.logger.info(
            "Loaded records",
            extra={"context": {**context, "format": detected_format, **counters, "dataset_version": resolved_version, "year": inferred_year}},
        )

        artifacts: dict[str, str] = {}
        checksum = ""
        dataset_version_id: int | None = None

        if not dry_run:
            if inferred_year is None:
                raise ValueError("Year must be provided for persistence")
            repo = DatabaseRepository(self.settings.postgres)
            storage = StorageClient.from_settings(self.settings)
            lineage = LineageRecorder(storage)

            with repo.connection() as conn:
                dataset_version_id = repo.create_dataset_version(
                    conn,
                    version_name=resolved_version,
                    year=int(inferred_year),
                    source_uri=str(source),
                )
                repo.copy_flights_raw(conn, dataset_version_id=dataset_version_id, table=table)
                repo.upsert_flights_norm(conn, dataset_version_id=dataset_version_id, table=table)

            raw_uri = storage.upload_csv(
                int(inferred_year),
                resolved_version,
                "raw",
                raw_df,
                prefix=storage_prefix,
            )
            normalized_uri = storage.upload_parquet(
                int(inferred_year),
                resolved_version,
                "normalized",
                table,
                prefix=storage_prefix,
            )
            lineage_payload = lineage.record(
                source=str(source),
                year=int(inferred_year),
                version=resolved_version,
                raw_data=raw_df,
                normalized=table,
                counters=counters,
                raw_uri=raw_uri,
                normalized_uri=normalized_uri,
                prefix=storage_prefix,
            )
            artifacts = {
                "raw": raw_uri,
                "normalized": normalized_uri,
                "lineage": lineage_payload.get("uri", ""),
            }
            checksum = lineage_payload.get("checksum", "")

            with repo.connection() as conn:
                repo.mark_ingested(
                    conn,
                    dataset_version_id=dataset_version_id,
                    checksum=checksum,
                    artifacts=artifacts,
                )

        self.logger.debug("Pipeline configuration", extra={"context": self.settings.model_dump()})

        result_context = {
            **context,
            "dataset_version": resolved_version,
            "year": inferred_year,
            "dataset_version_id": dataset_version_id,
            "artifacts": artifacts,
            "checksum": checksum,
        }
        self.logger.info("Finished ingest pipeline", extra={"context": result_context})
        return table, counters

    def _generate_version(self) -> str:
        """Generate dataset version identifier when not provided."""

        return pendulum.now("UTC").format(self._VERSION_FORMAT)

    def _infer_year(self, table: pa.Table) -> Optional[int]:
        """Infer dataset year from the normalized Arrow table."""

        if table.num_rows == 0:
            return None
        if "start_time_utc" not in table.column_names:
            return None
        df = pl.from_arrow(table)
        if "superseded" in df.columns:
            df = df.filter(~pl.col("superseded"))
        if df.is_empty():
            return None
        try:
            year_series = df.select(pl.col("start_time_utc").dt.year().drop_nulls()).to_series()
            if year_series.is_empty():
                return None
            return int(year_series.item())
        except Exception:  # pragma: no cover - best effort
            return None
