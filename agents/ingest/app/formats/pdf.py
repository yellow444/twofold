"""PDF format loader with graceful degradation."""

from __future__ import annotations

import io
import logging
import re
from pathlib import Path
from typing import Any, List

import polars as pl

from . import FormatHandler, FormatReadResult, register_handler
from ._common import sanitize

logger = logging.getLogger("app.formats.pdf")

try:  # pragma: no cover - optional dependency
    import camelot  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    camelot = None

try:  # pragma: no cover - optional dependency
    import tabula  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tabula = None


_PDF_SUFFIXES = {".pdf"}


def _detect(source: str | Path) -> bool:
    return Path(str(source)).suffix.lower() in _PDF_SUFFIXES


def _load_with_camelot(path: Path) -> list[pl.DataFrame]:  # pragma: no cover - optional
    tables: list[pl.DataFrame] = []
    if not camelot:
        return tables
    try:
        result = camelot.read_pdf(str(path), pages="all")
    except Exception as exc:  # pragma: no cover - runtime logging
        logger.warning("Camelot failed to parse PDF %s: %s", path, exc)
        return tables
    for table in result:
        frame = table.df.astype(str)
        df = pl.from_pandas(frame, include_index=False)
        tables.append(sanitize(df))
    return tables


def _load_with_tabula(path: Path) -> list[pl.DataFrame]:  # pragma: no cover - optional
    tables: list[pl.DataFrame] = []
    if not tabula:
        return tables
    try:
        result = tabula.read_pdf(str(path), pages="all", multiple_tables=True)
    except Exception as exc:  # pragma: no cover - runtime logging
        logger.warning("Tabula failed to parse PDF %s: %s", path, exc)
        return tables
    for frame in result:
        df = pl.from_pandas(frame.astype(str), include_index=False)
        tables.append(sanitize(df))
    return tables


_TEXT_TABLE_PATTERN = re.compile(r"^[^\S\n]*[\w\d]+([^\S\n]*[,;\t][^\S\n]*[\w\d]+)+[^\S\n]*$")


def _fallback_text(path: Path) -> pl.DataFrame:
    """Fallback parser converting text-like PDFs into a dataframe."""

    try:
        raw = path.read_bytes()
    except OSError as exc:  # pragma: no cover - filesystem failure
        logger.error("Failed to read PDF %s: %s", path, exc)
        return pl.DataFrame()

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="ignore")

    table_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _TEXT_TABLE_PATTERN.match(stripped):
            table_lines.append(stripped)

    if not table_lines:
        return pl.DataFrame()

    delimiter = ","
    if all(";" in line for line in table_lines):
        delimiter = ";"
    elif all("\t" in line for line in table_lines):
        delimiter = "\t"

    pseudo_csv = "\n".join(table_lines)
    buffer = io.StringIO(pseudo_csv)
    df = pl.read_csv(buffer, separator=delimiter)
    return sanitize(df)


def _load(source: str | Path) -> FormatReadResult:
    path = Path(str(source))
    metadata: dict[str, Any] = {"source_path": str(path)}

    tables: List[pl.DataFrame] = []
    tables.extend(_load_with_camelot(path))
    if not tables:
        tables.extend(_load_with_tabula(path))

    if tables:
        combined = pl.concat(tables, how="vertical", rechunk=True)
        combined = sanitize(combined)
        return FormatReadResult(records=combined, metadata=metadata)

    logger.warning("Falling back to text extraction for PDF %s", path)
    metadata["degraded"] = True
    df = _fallback_text(path)
    df = sanitize(df)
    return FormatReadResult(records=df, metadata=metadata)


register_handler(
    FormatHandler(
        name="pdf",
        aliases=("application/pdf",),
        loader=_load,
        detector=_detect,
    )
)
