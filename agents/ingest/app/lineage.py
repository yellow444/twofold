"""Lineage report generation for ingest runs."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
import pyarrow as pa

from .storage import StorageClient


@dataclass(slots=True)
class LineageRecorder:
    """Collect lineage information and persist it to object storage."""

    storage: StorageClient
    schema_version: str = "1.0"

    def record(
        self,
        *,
        source: str,
        year: int,
        version: str,
        raw_data: pl.DataFrame | pa.Table | None,
        normalized: pa.Table,
        counters: Dict[str, int],
        raw_uri: str,
        normalized_uri: str,
        prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build and persist lineage data returning the payload."""

        checksum = compute_file_checksum(source)
        raw_df = ensure_polars(raw_data)
        norm_df = ensure_polars(normalized)
        valid_df = norm_df.filter(~pl.col("superseded")) if "superseded" in norm_df.columns else norm_df

        lineage_uri = self.storage.build_uri(year, version, "lineage", "json", prefix=prefix)
        payload: Dict[str, Any] = {
            "schema_version": self.schema_version,
            "source": {
                "path": str(source),
                "checksum": checksum,
            },
            "counts": {
                "raw": int(raw_df.height),
                "normalized": int(norm_df.height),
                "valid": int(valid_df.height),
                "invalid": int(counters.get("invalid", 0)),
                "duplicates": int(counters.get("duplicates", 0)),
            },
            "artifacts": {
                "raw": raw_uri,
                "normalized": normalized_uri,
                "lineage": lineage_uri,
            },
        }
        self.storage.upload_json(year, version, "lineage", payload, prefix=prefix)
        payload["uri"] = lineage_uri
        payload["checksum"] = checksum
        return payload


def ensure_polars(data: pl.DataFrame | pa.Table | None) -> pl.DataFrame:
    """Convert arbitrary tabular data to a Polars DataFrame."""

    if data is None:
        return pl.DataFrame()
    if isinstance(data, pl.DataFrame):
        return data
    return pl.from_arrow(data)


def compute_file_checksum(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate SHA256 checksum for the provided file path."""

    file_path = Path(path)
    if not file_path.exists():
        return ""
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
