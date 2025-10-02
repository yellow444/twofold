"""Uniqueness check for normalized flights."""

from __future__ import annotations

import pandas as pd

from .base import CheckResult, CheckStatus, DataCheck
from .utils import dataframe_to_records


class UniquenessCheck(DataCheck):
    """Validate uniqueness of key dimensions."""

    name = "uniqueness"
    _key_columns = ("flight_id", "start_time_utc", "region_code")

    def run(self, data: pd.DataFrame) -> CheckResult:
        missing = [col for col in self._key_columns if col not in data.columns]
        if missing:
            return CheckResult(
                name=self.name,
                status=CheckStatus.FAIL,
                summary=f"missing key columns: {', '.join(missing)}",
            )

        duplicates = data.duplicated(subset=list(self._key_columns), keep=False)
        duplicate_rows = data[duplicates]
        if duplicate_rows.empty:
            return CheckResult(
                name=self.name,
                status=CheckStatus.OK,
                summary="primary key combination is unique",
            )

        return CheckResult(
            name=self.name,
            status=CheckStatus.FAIL,
            summary=f"found {duplicate_rows.shape[0]} duplicate key rows",
            details={
                "duplicate_count": int(duplicate_rows.shape[0]),
                "sample_rows": dataframe_to_records(duplicate_rows),
            },
        )


def build_uniqueness_check() -> UniquenessCheck:
    """Factory returning uniqueness check instance."""

    return UniquenessCheck()


__all__ = ["UniquenessCheck", "build_uniqueness_check"]
