"""CSV format loader with streaming support."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from . import FormatHandler, FormatReadResult, register_handler
from ._common import sanitize


def _detect(source: str | Path) -> bool:
    return Path(str(source)).suffix.lower() == ".csv"


def _load(source: str | Path) -> FormatReadResult:
    path = Path(str(source))
    scan = pl.scan_csv(
        path,
        has_header=True,
        infer_schema_length=1024,
        ignore_errors=True,
        null_values=["", "null", "NULL"],
    )
    df = scan.collect(streaming=True)
    df = sanitize(df)
    metadata: dict[str, Any] = {"source_path": str(path)}
    return FormatReadResult(records=df, metadata=metadata)


register_handler(
    FormatHandler(
        name="csv",
        aliases=("text/csv",),
        loader=_load,
        detector=_detect,
    )
)
