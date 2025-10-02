"""Base utilities shared by all quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

import pandas as pd


class CheckStatus(str, Enum):
    """Standardized status levels for data quality checks."""

    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(slots=True)
class CheckResult:
    """Outcome of a single check."""

    name: str
    status: CheckStatus
    summary: str
    details: dict[str, Any] | None = None


class DataCheck(Protocol):
    """Common protocol implemented by all checks."""

    name: str

    def run(self, data: pd.DataFrame) -> CheckResult:
        """Validate ``data`` and return the :class:`CheckResult`."""


__all__ = ["CheckStatus", "CheckResult", "DataCheck"]
