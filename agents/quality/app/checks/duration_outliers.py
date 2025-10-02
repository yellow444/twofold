"""Detect duration outliers using an IQR strategy."""

from __future__ import annotations

import pandas as pd

from .base import CheckResult, CheckStatus, DataCheck
from .utils import dataframe_to_records


class DurationOutlierCheck(DataCheck):
    """Identify unusually long or short flights."""

    name = "duration_outliers"

    def run(self, data: pd.DataFrame) -> CheckResult:
        if "duration_minutes" not in data.columns:
            return CheckResult(
                name=self.name,
                status=CheckStatus.FAIL,
                summary="duration_minutes column missing",
            )

        durations = data["duration_minutes"].dropna()
        if durations.empty:
            return CheckResult(
                name=self.name,
                status=CheckStatus.WARN,
                summary="no duration values available for outlier detection",
            )

        q1 = durations.quantile(0.25)
        q3 = durations.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outliers = data[(data["duration_minutes"] < lower) | (data["duration_minutes"] > upper)]
        detail = {
            "thresholds": {"lower": float(lower), "upper": float(upper)},
            "outlier_count": int(outliers.shape[0]),
        }

        if outliers.empty:
            return CheckResult(
                name=self.name,
                status=CheckStatus.OK,
                summary="no duration outliers detected",
                details=detail,
            )

        detail["sample_rows"] = dataframe_to_records(outliers)
        return CheckResult(
            name=self.name,
            status=CheckStatus.WARN,
            summary=f"detected {outliers.shape[0]} potential duration outliers",
            details=detail,
        )


def build_duration_outlier_check() -> DurationOutlierCheck:
    """Factory returning the duration outlier check."""

    return DurationOutlierCheck()


__all__ = ["DurationOutlierCheck", "build_duration_outlier_check"]
