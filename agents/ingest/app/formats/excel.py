"""Excel format loader using pandas/openpyxl."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from . import FormatHandler, FormatReadResult, register_handler
from ._common import sanitize


EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}


def _detect(source: str | Path) -> bool:
    return Path(str(source)).suffix.lower() in EXCEL_SUFFIXES


def _load(source: str | Path) -> FormatReadResult:
    path = Path(str(source))
    try:
        frame = pd.read_excel(path, dtype=str)
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "Reading Excel files requires optional dependencies (openpyxl or xlrd)."
        ) from exc
    except ValueError:
        frame = pd.read_excel(path, dtype=str, engine="openpyxl")
    df = pl.from_pandas(frame, include_index=False)
    df = sanitize(df)
    metadata: dict[str, Any] = {"source_path": str(path)}
    return FormatReadResult(records=df, metadata=metadata)


register_handler(
    FormatHandler(
        name="xlsx",
        aliases=("xls", "application/vnd.ms-excel", "application/vnd.openxmlformats"),
        loader=_load,
        detector=_detect,
    )
)
