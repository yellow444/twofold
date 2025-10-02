"""Validate latitude/longitude ranges."""

from __future__ import annotations

import pandas as pd

from .base import CheckResult, CheckStatus, DataCheck
from .utils import dataframe_to_records


class CoordinateRangeCheck(DataCheck):
    """Ensure latitude and longitude stay within geographic bounds."""

    name = "coordinate_range"

    def run(self, data: pd.DataFrame) -> CheckResult:
        lat = data.get("latitude")
        lon = data.get("longitude")
        detail = {}

        if lat is None or lon is None:
            return CheckResult(
                name=self.name,
                status=CheckStatus.WARN,
                summary="latitude/longitude columns not available",
            )

        invalid = data[
            (lat.notna() & ((lat < -90) | (lat > 90)))
            | (lon.notna() & ((lon < -180) | (lon > 180)))
        ]

        if invalid.empty:
            summary = "coordinates fall within expected bounds"
            status = CheckStatus.OK
        else:
            status = CheckStatus.FAIL
            summary = f"found {invalid.shape[0]} coordinates outside geographic bounds"
            detail = {
                "invalid_count": int(invalid.shape[0]),
                "sample_rows": dataframe_to_records(invalid),
            }

        return CheckResult(name=self.name, status=status, summary=summary, details=detail or None)


def build_coordinate_range_check() -> CoordinateRangeCheck:
    """Factory returning a coordinate range check instance."""

    return CoordinateRangeCheck()


__all__ = ["CoordinateRangeCheck", "build_coordinate_range_check"]
