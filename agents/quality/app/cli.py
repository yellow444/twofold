"""Command line interface for the quality validation agent."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from psycopg.conninfo import make_conninfo

from .checks import CheckStatus
from .config import QualitySettings, get_settings
from .logging import configure_logging, get_logger
from .pipeline import QualityReport, run_pipeline
from .repository import QualityRepository

app = typer.Typer(help="Quality validation utilities for normalized flights.")
LOGGER = get_logger(__name__)


def _default_output_path(settings: QualitySettings, dataset_version: str | None) -> Path:
    suffix = dataset_version or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return settings.resolved_artifacts_dir / f"quality_report_{suffix}.json"


def _violations_output_path(report_path: Path) -> Path:
    return report_path.with_name(report_path.stem.replace("report", "violations") + ".json")


def _override_settings(
    settings: QualitySettings,
    *,
    database_url: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    sslmode: Optional[str] = None,
) -> QualitySettings:
    """Return a copy of settings with database URL overrides applied."""

    base_url = database_url or str(settings.database_url)
    overrides = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": sslmode,
    }
    filtered = {key: value for key, value in overrides.items() if value is not None}
    if filtered:
        base_url = make_conninfo(base_url, **filtered)
    if database_url or filtered:
        return settings.model_copy(update={"database_url": base_url})
    return settings


@app.command()
def validate(
    dataset_version: Optional[str] = typer.Option(
        None,
        "--dataset-version",
        "-d",
        help="Dataset version identifier to validate.",
    ),
    dry_run: bool = typer.Option(
        False, help="Run validation without writing to the database. Artifacts are still saved."
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional path for the JSON report. Defaults to the artifacts directory.",
    ),
    database_url: Optional[str] = typer.Option(None, help="Override Postgres DSN."),
    db_host: Optional[str] = typer.Option(None, help="Postgres hostname override."),
    db_port: Optional[int] = typer.Option(None, help="Postgres port override."),
    db_name: Optional[str] = typer.Option(None, help="Postgres database name override."),
    db_user: Optional[str] = typer.Option(None, help="Postgres user override."),
    db_password: Optional[str] = typer.Option(None, help="Postgres password override."),
    db_sslmode: Optional[str] = typer.Option(None, help="Postgres sslmode override."),
) -> None:
    """Validate a dataset and emit a structured report."""

    configure_logging()
    settings = get_settings()
    settings = _override_settings(
        settings,
        database_url=database_url,
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        sslmode=db_sslmode,
    )
    LOGGER.debug("Loaded settings", extra={"settings": settings.model_dump()})

    repository = QualityRepository(str(settings.database_url))
    report: QualityReport = run_pipeline(
        settings,
        dataset_version,
        repository=repository,
        dry_run=dry_run,
    )
    typer.echo(report.quality_status)

    output_path = output or _default_output_path(settings, report.dataset_version)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(report.to_dict(), fp, indent=2, ensure_ascii=False)
    violations_path = _violations_output_path(output_path)
    with violations_path.open("w", encoding="utf-8") as fp:
        json.dump(
            {
                "warn_count": report.warn_count,
                "fail_count": report.fail_count,
                "total": report.violation_count,
                "quality_status": report.quality_status,
            },
            fp,
            indent=2,
            ensure_ascii=False,
        )
    LOGGER.info(
        "Artifacts written",
        extra={"report_path": str(output_path), "violations_path": str(violations_path)},
    )

    exit_code = 0 if report.status is not CheckStatus.FAIL else 1
    raise typer.Exit(code=exit_code)


def main() -> None:
    """Entrypoint for ``python -m agents.quality.app.cli``."""

    app()


if __name__ == "__main__":
    main()
