"""Canonical schemas and column mappings for ingest records."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, MutableMapping, TypedDict

from .utils import normalize_header


class FlightRecord(TypedDict, total=False):
    """Canonical representation of a single flight activity record."""

    flight_id: str
    surrogate_id: str | None
    start_time_utc: datetime
    end_time_utc: datetime
    duration_minutes: float
    region_code: str | None
    region_name: str | None
    latitude: float | None
    longitude: float | None
    vehicle_category: str | None
    operator_type: str | None
    flight_purpose: str | None
    payload_type: str | None
    superseded: bool


CANONICAL_ORDER: list[str] = [
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
"""Preferred order of columns for downstream consumers."""


NUMERIC_FIELDS: set[str] = {"duration_minutes", "latitude", "longitude"}
"""Columns that should be interpreted as numeric values."""


DATETIME_FIELDS: tuple[str, ...] = ("start_time", "end_time")
"""Canonical datetime column names in source data before conversion to UTC."""


COLUMN_ALIASES: Dict[str, set[str]] = {
    "flight_id": {"flight_id", "flightno", "flight_no", "flightnumber", "номерполета", "flight"},
    "start_time": {
        "start_time",
        "start",
        "start_datetime",
        "departure_time",
        "времянчала",
        "временавылета",
    },
    "end_time": {
        "end_time",
        "end",
        "end_datetime",
        "arrival_time",
        "времязавершения",
        "времепосадки",
    },
    "duration_minutes": {
        "duration",
        "duration_minutes",
        "duration_min",
        "продолжительность",
        "длительностьмин",
    },
    "region_code": {"region_code", "region", "regionid", "кодрегиона"},
    "region_name": {"region_name", "region_title", "названиерегиона"},
    "latitude": {"latitude", "lat", "широта"},
    "longitude": {"longitude", "lon", "lng", "долгота"},
    "vehicle_category": {"vehicle_category", "uav_type", "типбпла"},
    "operator_type": {"operator_type", "operator", "типоператора"},
    "flight_purpose": {"flight_purpose", "purpose", "цельвылета"},
    "payload_type": {"payload_type", "payload", "типнагрузки"},
}
"""Mapping between canonical column names and their possible aliases in raw sources."""


CANONICAL_STRINGS: tuple[str, ...] = (
    "flight_id",
    "surrogate_id",
    "region_code",
    "region_name",
    "vehicle_category",
    "operator_type",
    "flight_purpose",
    "payload_type",
)


BOOLEAN_FIELDS: tuple[str, ...] = ("superseded",)


def build_reverse_column_map() -> Dict[str, str]:
    """Create a lookup map from normalized aliases to canonical names."""

    reverse: Dict[str, str] = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            reverse[normalize_header(alias)] = canonical
    return reverse


REVERSE_COLUMN_MAP: Dict[str, str] = build_reverse_column_map()
"""Normalized alias → canonical column lookup map."""


def normalize_columns(columns: Iterable[str]) -> Dict[str, str]:
    """Return a rename mapping converting arbitrary columns to canonical ones."""

    mapping: MutableMapping[str, str] = {}
    for name in columns:
        normalized = normalize_header(name)
        canonical = REVERSE_COLUMN_MAP.get(normalized)
        if canonical:
            mapping[name] = canonical
    return dict(mapping)
