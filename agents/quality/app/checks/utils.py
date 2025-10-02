"""Utility helpers for check implementations."""

from __future__ import annotations

from typing import Any

import pandas as pd


def dataframe_to_records(frame: pd.DataFrame, limit: int = 5) -> list[dict[str, Any]]:
    """Convert a dataframe slice into JSON-serialisable records."""

    serialisable = frame.head(limit).copy()
    for column in serialisable.select_dtypes(include=["datetime", "datetimetz"]).columns:
        series = serialisable[column]
        if series.dt.tz is None:
            series = series.dt.tz_localize("UTC")
        serialisable[column] = series.dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return serialisable.where(pd.notna(serialisable), None).to_dict(orient="records")


__all__ = ["dataframe_to_records"]
