"""Collection of quality checks."""

from __future__ import annotations

from .base import CheckResult, CheckStatus, DataCheck
from .coordinate_range import CoordinateRangeCheck, build_coordinate_range_check
from .duration_outliers import DurationOutlierCheck, build_duration_outlier_check
from .duration_range import DurationRangeCheck, build_duration_range_check
from .monthly_completeness import MonthlyCompletenessCheck, build_monthly_completeness_check
from .schema import SchemaCheck, build_schema_check
from .uniqueness import UniquenessCheck, build_uniqueness_check


def default_checks() -> list[DataCheck]:
    """Return the default suite of quality checks."""

    return [
        build_schema_check(),
        build_duration_range_check(),
        build_coordinate_range_check(),
        build_uniqueness_check(),
        build_monthly_completeness_check(),
        build_duration_outlier_check(),
    ]


__all__ = [
    "CheckResult",
    "CheckStatus",
    "DataCheck",
    "CoordinateRangeCheck",
    "DurationOutlierCheck",
    "DurationRangeCheck",
    "MonthlyCompletenessCheck",
    "SchemaCheck",
    "UniquenessCheck",
    "default_checks",
]
