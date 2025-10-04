"""Check completeness of months within each year."""

from __future__ import annotations

import pandas as pd

from .base import CheckResult, CheckStatus, DataCheck


class MonthlyCompletenessCheck(DataCheck):
    """Ensure each year contains a contiguous sequence of months."""

    name = "monthly_completeness"

    def run(self, data: pd.DataFrame) -> CheckResult:
        if "start_time_utc" not in data.columns:
            return CheckResult(
                name=self.name,
                status=CheckStatus.FAIL,
                summary="start_time_utc column missing",
            )

        timestamps = pd.to_datetime(data["start_time_utc"], errors="coerce")
        invalid_dates = data[timestamps.isna()]
        if not invalid_dates.empty:
            return CheckResult(
                name=self.name,
                status=CheckStatus.FAIL,
                summary="invalid datetimes in start_time_utc",
                details={
                    "invalid_count": int(invalid_dates.shape[0]),
                    "sample_rows": invalid_dates.head(5).to_dict(orient="records"),
                },
            )

        months = timestamps.dt.month
        years = timestamps.dt.year
        detail: dict[str, list[int]] = {}
        warnings: dict[int, list[int]] = {}

        for year in sorted(years.unique()):
            year_months = sorted(months[years == year].unique())
            if not year_months:
                continue
            expected = set(range(min(year_months), max(year_months) + 1))
            missing = sorted(expected.difference(year_months))
            if missing:
                warnings[year] = missing
            detail[str(year)] = year_months

        if warnings:
            return CheckResult(
                name=self.name,
                status=CheckStatus.WARN,
                summary="missing intermediate months detected",
                details={"missing_months": warnings, "observed": detail},
            )

        return CheckResult(
            name=self.name,
            status=CheckStatus.OK,
            summary="months are contiguous within each year",
            details={"observed": detail} if detail else None,
        )


def build_monthly_completeness_check() -> MonthlyCompletenessCheck:
    """Factory returning the monthly completeness check."""

    return MonthlyCompletenessCheck()


__all__ = ["MonthlyCompletenessCheck", "build_monthly_completeness_check"]
