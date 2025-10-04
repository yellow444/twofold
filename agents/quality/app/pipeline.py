"""Co-ordinate data loading, validation checks and reporting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator

import pandas as pd
import psycopg

from agents.ingest.app.schemas import CANONICAL_ORDER

from .checks import CheckResult, CheckStatus, DataCheck, default_checks
from .config import QualitySettings
from .logging import get_logger
from .repository import (
    FlightQualityIssue,
    QualityReportEntry,
    QualityRepository,
)

LOGGER = get_logger(__name__)

DataLoader = Callable[[QualitySettings, str | None], pd.DataFrame]


QUALITY_STATUS_MAP: dict[CheckStatus, str] = {
    CheckStatus.OK: "validated",
    CheckStatus.WARN: "quality_warn",
    CheckStatus.FAIL: "quality_fail",
}


@dataclass(slots=True)
class QualityReport:
    """Serializable report describing the results of the validation run."""

    dataset_version: str | None
    generated_at: datetime
    status: CheckStatus
    quality_status: str
    checks: list[CheckResult]
    entries: list[QualityReportEntry]
    flight_issues: list[FlightQualityIssue]
    warn_count: int
    fail_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON serialisable representation."""

        return {
            "dataset_version": self.dataset_version,
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "quality_status": self.quality_status,
            "warn_count": self.warn_count,
            "fail_count": self.fail_count,
            "checks": [
                {
                    "name": check.name,
                    "status": check.status.value,
                    "summary": check.summary,
                    "details": check.details,
                }
                for check in self.checks
            ],
            "entries": [
                {
                    "check_name": entry.check_name,
                    "severity": entry.severity.value,
                    "details": entry.payload,
                }
                for entry in self.entries
            ],
            "flight_issues": [
                {
                    "flight_uid": issue.flight_uid,
                    "check_name": issue.check_name,
                    "severity": issue.severity.value,
                    "details": issue.details,
                }
                for issue in self.flight_issues
            ],
        }

    @property
    def violation_count(self) -> int:
        """Return the total number of WARN/FAIL checks."""

        return self.warn_count + self.fail_count


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


def _iter_sample_rows(details: dict[str, Any] | None) -> Iterator[dict[str, Any]]:
    """Yield dict rows stored inside the ``details`` payload (if any)."""

    if not details:
        return
    for key in ("sample_rows", "rows", "violations"):
        value = details.get(key)
        if isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    yield row


def summarise_results(
    results: Iterable[CheckResult],
) -> tuple[list[QualityReportEntry], list[FlightQualityIssue]]:
    """Convert raw :class:`CheckResult` objects into persistence payloads."""

    entries: list[QualityReportEntry] = []
    flight_issues: list[FlightQualityIssue] = []
    for result in results:
        details = result.details or {}
        payload: dict[str, Any] = {"summary": result.summary}
        if details:
            payload["details"] = details

        rows = list(_iter_sample_rows(details))
        impacted_regions = sorted(
            {str(row["region_code"]) for row in rows if row.get("region_code")}
        )
        impacted_records = sorted(
            {str(row["flight_id"]) for row in rows if row.get("flight_id")}
        )
        if impacted_regions:
            payload["impacted_regions"] = impacted_regions
        if impacted_records:
            payload["impacted_records"] = impacted_records

        entries.append(
            QualityReportEntry(
                check_name=result.name,
                severity=result.status,
                payload=payload,
            )
        )
        if result.status in {CheckStatus.WARN, CheckStatus.FAIL}:
            for flight_uid in impacted_records:
                issue_details = {"summary": result.summary}
                if details:
                    issue_details["details"] = details
                flight_issues.append(
                    FlightQualityIssue(
                        flight_uid=flight_uid,
                        check_name=result.name,
                        severity=result.status,
                        details=issue_details,
                    )
                )
    return entries, flight_issues


def run_pipeline(
    settings: QualitySettings,
    dataset_version: str | None = None,
    *,
    loader: DataLoader | None = None,
    checks: Iterable[DataCheck] | None = None,
    repository: QualityRepository | None = None,
    dry_run: bool = False,
) -> QualityReport:
    """Execute the full validation flow."""

    dataset = dataset_version or settings.default_dataset_version
    data_loader = loader or load_flights
    active_checks = list(checks or default_checks())

    data = data_loader(settings, dataset)
    results = run_checks(data, active_checks)
    status = aggregate_status(results)
    entries, flight_issues = summarise_results(results)
    warn_count = sum(1 for entry in entries if entry.severity is CheckStatus.WARN)
    fail_count = sum(1 for entry in entries if entry.severity is CheckStatus.FAIL)
    quality_status = QUALITY_STATUS_MAP[status]

    report = QualityReport(
        dataset_version=dataset,
        generated_at=datetime.now(timezone.utc),
        status=status,
        quality_status=quality_status,
        checks=results,
        entries=entries,
        flight_issues=flight_issues,
        warn_count=warn_count,
        fail_count=fail_count,
    )
    LOGGER.info("Validation complete", extra={"dataset_version": dataset, "status": status.value})

    if repository and not dry_run and dataset:
        LOGGER.info(
            "Persisting quality results",
            extra={
                "dataset_version": dataset,
                "warn_count": warn_count,
                "fail_count": fail_count,
                "quality_status": quality_status,
            },
        )
        with repository.connection() as conn:
            dataset_id = repository.fetch_dataset_version_id(conn, dataset)
            repository.replace_quality_reports(conn, dataset_id, entries)
            repository.replace_flight_quality_issues(conn, dataset_id, flight_issues)
            repository.update_dataset_version(
                conn,
                dataset_id,
                status=quality_status,
                warn_count=warn_count,
                fail_count=fail_count,
            )
    elif repository and not dataset:
        LOGGER.warning(
            "Dataset version missing; skipping persistence",
            extra={"dry_run": dry_run},
        )
    elif dry_run:
        LOGGER.info("Dry run enabled; database writes skipped")

    return report


__all__ = [
    "QualityReport",
    "aggregate_status",
    "summarise_results",
    "load_flights",
    "run_checks",
    "run_pipeline",
]
