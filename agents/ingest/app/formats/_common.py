"""Shared helpers for format loaders."""

from __future__ import annotations

import logging
from typing import Sequence

import polars as pl

from ..schemas import normalize_columns
from ..utils import ensure_unique, normalize_header

logger = logging.getLogger("app.formats")


SERVICE_ROW_PATTERNS: tuple[str, ...] = (
    "page ",
    "страница",
    "generated",
    "source:",
)


def ensure_polars(data: pl.DataFrame | Sequence[dict[str, object]] | None) -> pl.DataFrame:
    """Convert arbitrary tabular data to a `polars.DataFrame`."""

    if data is None:
        return pl.DataFrame()
    if isinstance(data, pl.DataFrame):
        return data
    return pl.DataFrame(data)


def clean_headers(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize headers to snake_case and ensure uniqueness."""

    renamed = normalize_columns(df.columns)
    columns = [normalize_header(name) for name in df.columns]
    columns = ensure_unique(columns)
    df = df.rename(dict(zip(df.columns, columns)))
    if renamed:
        df = df.rename({normalize_header(src): dst for src, dst in renamed.items()})
    return df


def strip_whitespace(df: pl.DataFrame) -> pl.DataFrame:
    """Strip surrounding whitespace from string columns."""

    string_columns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    if not string_columns:
        return df
    return df.with_columns([pl.col(col).str.strip().alias(col) for col in string_columns])


def normalize_decimals(df: pl.DataFrame) -> pl.DataFrame:
    """Replace comma decimal separators with dots for string columns."""

    string_columns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    if not string_columns:
        return df
    updates = []
    for col in string_columns:
        updates.append(pl.col(col).str.replace_all(",", ".").alias(col))
    return df.with_columns(updates)


def drop_service_rows(df: pl.DataFrame, limit: int = 10) -> pl.DataFrame:
    """Trim leading service rows (watermarks, page markers)."""

    drop_count = 0
    for row in df.head(limit).iter_rows():
        values = [str(value).strip().lower() for value in row if value is not None]
        if not values or all(value == "" for value in values):
            drop_count += 1
            continue
        if any(value.startswith(pattern) for pattern in SERVICE_ROW_PATTERNS for value in values):
            drop_count += 1
            continue
        break
    if drop_count:
        df = df.slice(drop_count, df.height - drop_count)
    return df


def sanitize(df: pl.DataFrame) -> pl.DataFrame:
    """Apply common sanitisation steps to the dataframe."""

    if df.is_empty():
        return df
    df = clean_headers(df)
    df = strip_whitespace(df)
    df = drop_service_rows(df)
    df = normalize_decimals(df)
    # strip whitespace again in case decimal normalization introduced spaces
    df = strip_whitespace(df)
    return df


def dataframe_from_rows(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> pl.DataFrame:
    """Construct a dataframe from row data ensuring canonical headers."""

    if not headers:
        return pl.DataFrame()
    normalized_headers = [normalize_header(header) for header in headers]
    normalized_headers = ensure_unique(normalized_headers)
    df = pl.DataFrame(rows, schema=list(normalized_headers))
    return sanitize(df)
