"""Core ingest pipeline implementation stub."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pyarrow as pa

from .config import AppSettings
from .formats import detect_format, load_records
from .logging import get_logger
from .normalization import normalize_records


class IngestPipeline:
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

        self.logger.info(
            "Loaded records",
            extra={"context": {**context, "format": detected_format, **counters}},
        )

        self.logger.debug("Pipeline configuration", extra={"context": self.settings.model_dump()})

        self.logger.info("Finished ingest pipeline", extra={"context": context})
        return table, counters
