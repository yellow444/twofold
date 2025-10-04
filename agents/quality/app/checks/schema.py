"""Schema validation check."""

from __future__ import annotations

from typing import Any

import pandas as pd

from agents.ingest.app.schemas import CANONICAL_ORDER

from .base import CheckResult, CheckStatus, DataCheck


class SchemaCheck(DataCheck):
    """Verify the dataframe adheres to the canonical schema."""

    name = "schema"
    _required_columns = {
        "flight_id",
        "start_time_utc",
        "end_time_utc",
        "duration_minutes",
    }
    _expected_columns = set(CANONICAL_ORDER)

    def run(self, data: pd.DataFrame) -> CheckResult:
        missing_columns = sorted(self._expected_columns.difference(data.columns))
        unexpected_columns = sorted(set(data.columns).difference(self._expected_columns))

        detail: dict[str, Any] = {}
        status = CheckStatus.OK
        messages: list[str] = []

        if missing_columns:
            status = CheckStatus.FAIL
            detail["missing_columns"] = missing_columns
            messages.append("missing required columns")

        if unexpected_columns:
            detail["unexpected_columns"] = unexpected_columns
            if status is CheckStatus.OK:
                status = CheckStatus.WARN
            messages.append("found unexpected columns")

        null_counts = {
            column: int(data[column].isna().sum())
            for column in self._required_columns
            if column in data.columns
        }
        null_violations = {col: count for col, count in null_counts.items() if count > 0}
        if null_violations:
            status = CheckStatus.FAIL
            detail["null_counts"] = null_violations
            messages.append("null values detected in required columns")

        if not messages:
            messages.append("schema matches canonical definition")

        return CheckResult(
            name=self.name,
            status=status,
            summary="; ".join(messages),
            details=detail or None,
        )


def build_schema_check() -> SchemaCheck:
    """Factory returning a schema check instance."""

    return SchemaCheck()


__all__ = ["SchemaCheck", "build_schema_check"]
