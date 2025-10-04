"""Validate duration ranges for flights."""

from __future__ import annotations

import pandas as pd

from .base import CheckResult, CheckStatus, DataCheck
from .utils import dataframe_to_records


class DurationRangeCheck(DataCheck):
    """Ensure flight durations fall within realistic boundaries."""

    name = "duration_range"

    def __init__(self, minimum: float = 1.0, maximum: float = 24 * 60) -> None:
        self.minimum = minimum
        self.maximum = maximum

    def run(self, data: pd.DataFrame) -> CheckResult:
        if "duration_minutes" not in data.columns:
            return CheckResult(
                name=self.name,
                status=CheckStatus.FAIL,
                summary="duration_minutes column missing",
            )

        durations = data["duration_minutes"].dropna()
        below = durations < self.minimum
        above = durations > self.maximum
        invalid_rows = data[below | above]
        detail = {
            "minimum": self.minimum,
            "maximum": self.maximum,
            "invalid_count": int(invalid_rows.shape[0]),
        }

        if invalid_rows.empty:
            summary = "all durations within expected range"
            status = CheckStatus.OK
        else:
            status = CheckStatus.FAIL
            summary = f"found {invalid_rows.shape[0]} durations outside range"
            detail["sample_rows"] = dataframe_to_records(invalid_rows)

        return CheckResult(name=self.name, status=status, summary=summary, details=detail)


def build_duration_range_check() -> DurationRangeCheck:
    """Factory returning a range check instance."""

    return DurationRangeCheck()


__all__ = ["DurationRangeCheck", "build_duration_range_check"]
