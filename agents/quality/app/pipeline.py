"""Co-ordinate data loading, validation checks and reporting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

import pandas as pd
import psycopg

from agents.ingest.app.schemas import CANONICAL_ORDER

from .checks import CheckResult, CheckStatus, DataCheck, default_checks
from .config import QualitySettings
from .logging import get_logger

LOGGER = get_logger(__name__)

DataLoader = Callable[[QualitySettings, str | None], pd.DataFrame]


@dataclass(slots=True)
class QualityReport:
    """Serializable report describing the results of the validation run."""

    dataset_version: str | None
    generated_at: datetime
    status: CheckStatus
    checks: list[CheckResult]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON serialisable representation."""

        return {
            "dataset_version": self.dataset_version,
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "checks": [
                {
                    "name": check.name,
                    "status": check.status.value,
                    "summary": check.summary,
                    "details": check.details,
                }
                for check in self.checks
            ],
        }


def load_flights(settings: QualitySettings, dataset_version: str | None) -> pd.DataFrame:
    """Load normalized flights from Postgres using psycopg."""

    query = [
        "SELECT",
        ", ".join(CANONICAL_ORDER),
        f"FROM {settings.database_schema}.{settings.table_name}",
    ]
    params: list[Any] = []
    if dataset_version:
        query.append("WHERE dataset_version = %s")
        params.append(dataset_version)

    sql = " ".join(query)
    LOGGER.info("Fetching flights from database", extra={"dataset_version": dataset_version})
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        frame = pd.read_sql(sql, conn, params=params or None)
    LOGGER.info("Loaded %s records", len(frame))
    return frame


def run_checks(data: pd.DataFrame, checks: Iterable[DataCheck]) -> list[CheckResult]:
    """Run the provided checks against ``data``."""

    results: list[CheckResult] = []
    for check in checks:
        LOGGER.debug("Running check", extra={"check": check.name})
        result = check.run(data)
        results.append(result)
        LOGGER.debug("Check result", extra={"check": check.name, "status": result.status.value})
    return results


def aggregate_status(results: Iterable[CheckResult]) -> CheckStatus:
    """Aggregate individual check results into an overall status."""

    final_status = CheckStatus.OK
    for result in results:
        if result.status is CheckStatus.FAIL:
            return CheckStatus.FAIL
        if result.status is CheckStatus.WARN and final_status is CheckStatus.OK:
            final_status = CheckStatus.WARN
    return final_status


def run_pipeline(
    settings: QualitySettings,
    dataset_version: str | None = None,
    *,
    loader: DataLoader | None = None,
    checks: Iterable[DataCheck] | None = None,
) -> QualityReport:
    """Execute the full validation flow."""

    dataset = dataset_version or settings.default_dataset_version
    data_loader = loader or load_flights
    active_checks = list(checks or default_checks())

    data = data_loader(settings, dataset)
    results = run_checks(data, active_checks)
    status = aggregate_status(results)

    report = QualityReport(
        dataset_version=dataset,
        generated_at=datetime.now(timezone.utc),
        status=status,
        checks=results,
    )
    LOGGER.info("Validation complete", extra={"dataset_version": dataset, "status": status.value})
    return report


__all__ = [
    "QualityReport",
    "aggregate_status",
    "load_flights",
    "run_checks",
    "run_pipeline",
]
