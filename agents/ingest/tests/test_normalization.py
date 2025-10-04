from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from app.formats import FormatReadResult
from app.normalization import normalize_records


def test_normalize_records_handles_timezone_duration_and_dedup() -> None:
    raw = pl.DataFrame(
        {
            "flight_id": ["A1", "A1", None, "A2"],
            "start_time": [
                "2024-01-01 10:00",
                "2024-01-01 10:00",
                "2024-01-01 12:00",
                "2024-01-01 14:00",
            ],
            "end_time": [
                "2024-01-01 11:00",
                "2024-01-01 11:30",
                "2024-01-01 12:45",
                "2024-01-01 14:30",
            ],
            "duration_minutes": [None, "90", "", "30"],
            "region_code": ["RU-MOW", "RU-MOW", None, "RU-MOW"],
            "region_name": ["Москва", "Москва", "Москва", "Москва"],
            "latitude": ["55,75", "55.75", "55.75", "95"],
            "longitude": ["37.61", "37.61", "37.61", "37.61"],
        }
    )

    result = FormatReadResult(records=raw, metadata={"report_timezone": "Europe/Moscow"})
    table, counters = normalize_records(result)

    assert counters == {"total": 4, "invalid": 1, "duplicates": 1}

    normalized = pl.from_arrow(table)
    assert normalized.height == 3
    assert set(normalized.columns) == set(
        [
            "flight_id",
            "surrogate_id",
            "start_time_utc",
            "end_time_utc",
            "duration_minutes",
            "region_code",
            "region_name",
            "latitude",
            "longitude",
            "vehicle_category",
            "operator_type",
            "flight_purpose",
            "payload_type",
            "superseded",
        ]
    )

    start_utc = normalized["start_time_utc"][0]
    assert start_utc.tzinfo is not None and start_utc.tzinfo.utcoffset(start_utc).total_seconds() == 0
    assert start_utc.isoformat().startswith("2024-01-01T07:00")

    assert normalized["duration_minutes"][0] == 60.0
    assert normalized["duration_minutes"][1] == 90.0

    surrogate_row = normalized.filter(pl.col("surrogate_id").is_not_null()).to_dicts()[0]
    assert surrogate_row["flight_id"] == surrogate_row["surrogate_id"]

    assert normalized.filter(pl.col("superseded")).height == 1
    assert normalized.filter(pl.col("latitude").is_null()).height == 0
    assert normalized.filter(pl.col("longitude").is_null()).height == 0
    assert normalized.filter(pl.col("flight_id") == "A2").height == 0
