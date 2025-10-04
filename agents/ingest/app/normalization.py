"""Normalization utilities for ingest records."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple
from uuid import NAMESPACE_URL, uuid5

import pendulum
import polars as pl
import pyarrow as pa

from .formats import FormatReadResult
from .schemas import (
    BOOLEAN_FIELDS,
    CANONICAL_ORDER,
    CANONICAL_STRINGS,
    NUMERIC_FIELDS,
    normalize_columns,
)


def _to_polars(data: pl.DataFrame | pa.Table) -> pl.DataFrame:
    if isinstance(data, pl.DataFrame):
        return data.clone()
    return pl.from_arrow(data)


def _parse_datetime(value: Any, tz_hint: str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            tz = tz_hint or "UTC"
            dt = pendulum.instance(value, tz=tz)
        else:
            dt = pendulum.instance(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = pendulum.parse(text, tz=tz_hint, strict=False)
        except Exception:
            return None
    if dt.tzinfo is None:
        if tz_hint:
            try:
                tz = pendulum.timezone(tz_hint)
            except Exception:
                tz = pendulum.UTC
            dt = dt.replace(tzinfo=tz)
        else:
            dt = dt.replace(tzinfo=pendulum.UTC)
    return dt.in_timezone(pendulum.UTC)


def _ensure_column(df: pl.DataFrame, name: str, dtype: pl.DataType, default: Any = None) -> pl.DataFrame:
    if name in df.columns:
        return df
    return df.with_columns(pl.lit(default, dtype=dtype).alias(name))


def _generate_surrogate(row: dict[str, Any]) -> str | None:
    if row.get("flight_id"):
        return None
    start = row.get("start_time_utc")
    end = row.get("end_time_utc")
    region = row.get("region_code") or row.get("region_name")
    if not start or not end:
        return None
    base = "|".join(
        str(part)
        for part in (start, end, region)
        if part is not None
    )
    if not base:
        return None
    return str(uuid5(NAMESPACE_URL, base))


def normalize_records(
    data: FormatReadResult | pl.DataFrame | pa.Table,
    *,
    report_timezone: str | None = None,
) -> Tuple[pa.Table, Dict[str, int]]:
    """Normalize records into the canonical Arrow table and compute counters."""

    metadata: Dict[str, Any] = {}
    if isinstance(data, FormatReadResult):
        metadata = dict(data.metadata)
        df = _to_polars(data.records)
        report_timezone = report_timezone or metadata.get("report_timezone")
    else:
        df = _to_polars(data)

    if df.is_empty():
        empty_table = pa.Table.from_pydict({col: [] for col in CANONICAL_ORDER})
        return empty_table, {"total": 0, "invalid": 0, "duplicates": 0}

    rename_map = normalize_columns(df.columns)
    if rename_map:
        df = df.rename(rename_map)

    total_records = df.height

    # Normalize strings
    for column in set(df.columns) & set(CANONICAL_STRINGS):
        df = df.with_columns(pl.col(column).cast(pl.Utf8, strict=False).str.strip().alias(column))

    # Parse datetime columns
    datetime_columns = [col for col in ("start_time", "end_time") if col in df.columns]
    for column in datetime_columns:
        target = f"{column}_utc"
        df = df.with_columns(
            pl.col(column)
            .map_elements(lambda value: _parse_datetime(value, report_timezone), return_dtype=pl.Datetime("us", "UTC"))
            .alias(target)
        )
    df = _ensure_column(df, "start_time_utc", pl.Datetime("us", "UTC"))
    df = _ensure_column(df, "end_time_utc", pl.Datetime("us", "UTC"))

    # Drop the original datetime columns to avoid ambiguity
    df = df.drop([col for col in ("start_time", "end_time") if col in df.columns])

    # Duration calculation
    duration_expr = (
        (pl.col("end_time_utc") - pl.col("start_time_utc")).dt.total_minutes()
    )
    if "duration_minutes" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("duration_minutes").cast(pl.Float64, strict=False).is_not_null())
            .then(pl.col("duration_minutes").cast(pl.Float64, strict=False))
            .otherwise(duration_expr)
            .alias("duration_minutes")
        )
    else:
        df = df.with_columns(duration_expr.alias("duration_minutes"))

    for numeric_column in NUMERIC_FIELDS:
        df = _ensure_column(df, numeric_column, pl.Float64, default=None)
        df = df.with_columns(pl.col(numeric_column).cast(pl.Float64, strict=False).alias(numeric_column))

    # Coordinate validation
    lat_valid = (pl.col("latitude").is_null() | pl.col("latitude").is_between(-90, 90))
    lon_valid = (pl.col("longitude").is_null() | pl.col("longitude").is_between(-180, 180))
    df = df.with_columns(
        pl.when(lat_valid).then(pl.col("latitude")).otherwise(pl.lit(None, dtype=pl.Float64)).alias("latitude"),
        pl.when(lon_valid).then(pl.col("longitude")).otherwise(pl.lit(None, dtype=pl.Float64)).alias("longitude"),
        (~lat_valid | ~lon_valid).alias("_invalid_coord"),
    )

    # Ensure string columns exist
    for column in CANONICAL_STRINGS:
        df = _ensure_column(df, column, pl.Utf8, default=None)
        df = df.with_columns(pl.col(column).cast(pl.Utf8, strict=False).alias(column))

    df = _ensure_column(df, "flight_id", pl.Utf8, default=None)

    df = df.with_columns(
        pl.struct(["flight_id", "start_time_utc", "end_time_utc", "region_code", "region_name"])
        .map_elements(_generate_surrogate, return_dtype=pl.Utf8)
        .alias("surrogate_id")
    )
    df = df.with_columns(
        pl.when(pl.col("flight_id").is_null() | (pl.col("flight_id").str.len_chars() == 0))
        .then(pl.col("surrogate_id"))
        .otherwise(pl.col("flight_id"))
        .alias("flight_id")
    )

    df = _ensure_column(df, "superseded", pl.Boolean, default=False)
    df = df.with_columns(pl.col("superseded").cast(pl.Boolean, strict=False).fill_null(False).alias("superseded"))

    invalid_mask = (
        pl.col("start_time_utc").is_null()
        | pl.col("end_time_utc").is_null()
        | (pl.col("end_time_utc") < pl.col("start_time_utc"))
    ) | pl.col("_invalid_coord")
    df = df.with_columns(invalid_mask.alias("_invalid"))
    invalid_count = df.filter(pl.col("_invalid")).height

    df = df.filter(~pl.col("_invalid"))

    # Deduplication
    key = pl.concat_str(
        [
            pl.col("flight_id").fill_null(""),
            pl.col("start_time_utc").dt.strftime("%Y-%m-%dT%H:%M:%S"),
            pl.col("region_code").fill_null(""),
        ],
        separator="|",
    )
    df = df.with_columns(
        key.alias("_dedupe_key"),
        pl.col("_dedupe_key").cumcount().over("_dedupe_key").alias("_dup_idx"),
        pl.count().over("_dedupe_key").alias("_dup_total"),
    )
    df = df.with_columns((pl.col("_dup_idx") < pl.col("_dup_total") - 1).alias("superseded"))
    duplicate_count = int(df.filter(pl.col("superseded")).height)
    df = df.drop(["_dedupe_key", "_dup_idx", "_dup_total", "_invalid", "_invalid_coord"])

    # Ensure canonical columns exist with defaults
    for column in CANONICAL_ORDER:
        if column not in df.columns:
            if column in NUMERIC_FIELDS:
                df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
            elif column in CANONICAL_STRINGS:
                df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(column))
            elif column in BOOLEAN_FIELDS:
                df = df.with_columns(pl.lit(False).alias(column))
            else:
                df = df.with_columns(pl.lit(None).alias(column))

    df = df.select(CANONICAL_ORDER)
    table = df.to_arrow()
    counters = {
        "total": total_records,
        "invalid": invalid_count,
        "duplicates": duplicate_count,
    }
    return table, counters
