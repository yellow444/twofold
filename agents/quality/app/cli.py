"""Command line interface for the quality validation agent."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from .config import QualitySettings, get_settings
from .logging import configure_logging, get_logger
from .pipeline import QualityReport, run_pipeline

app = typer.Typer(help="Quality validation utilities for normalized flights.")
LOGGER = get_logger(__name__)


def _default_output_path(settings: QualitySettings, dataset_version: str | None) -> Path:
    suffix = dataset_version or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return settings.resolved_artifacts_dir / f"quality_report_{suffix}.json"


@app.command()
def validate(
    dataset_version: Optional[str] = typer.Option(
        None,
        "--dataset-version",
        "-d",
        help="Dataset version identifier to validate.",
    ),
    dry_run: bool = typer.Option(False, help="Run validation without writing report."),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional path for the JSON report. Defaults to the artifacts directory.",
    ),
) -> None:
    """Validate a dataset and emit a structured report."""

    configure_logging()
    settings = get_settings()
    LOGGER.debug("Loaded settings", extra={"settings": settings.model_dump()})

    report: QualityReport = run_pipeline(settings, dataset_version)
    status = report.status.value
    typer.echo(status)

    if dry_run:
        LOGGER.info("Dry run requested; skipping report write")
    else:
        output_path = output or _default_output_path(settings, report.dataset_version)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(report.to_dict(), fp, indent=2, ensure_ascii=False)
        LOGGER.info("Report written", extra={"path": str(output_path)})

    raise typer.Exit(code=0 if status != "FAIL" else 1)


def main() -> None:
    """Entrypoint for ``python -m agents.quality.app.cli``."""

    app()


if __name__ == "__main__":
    main()
