"""Command-line interface for the ingest agent."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .config import AppSettings
from .logging import configure_logging, get_logger
from .pipeline import IngestPipeline

app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode="markdown")


@app.callback()
def init() -> None:
    """Initialize application-wide services before command execution."""

    configure_logging()


@app.command(help="Run the ingest pipeline for the given data source.")
def ingest(
    source: str = typer.Argument(..., help="Name of the data source to ingest."),
    year: Optional[int] = typer.Option(
        None,
        "--year",
        "-y",
        help="Target year for the ingest run.",
    ),
    fmt: Optional[str] = typer.Option(
        None,
        "--format",
        "-f",
        help="Explicit source format override (e.g. csv, xls).",
    ),
    storage_path: Optional[Path] = typer.Option(
        None,
        "--storage-path",
        "-s",
        help="Custom path in the storage backend where the dataset should be written.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Perform all preparation steps without persisting results.",
    ),
    dataset_version: Optional[str] = typer.Option(
        None,
        "--dataset-version",
        help="Override the dataset version stored in configuration.",
    ),
) -> None:
    """Execute the ingest pipeline with the provided parameters."""

    settings = AppSettings()
    pipeline = IngestPipeline(settings=settings)
    logger = get_logger("app.cli")

    context = {
        "source": source,
        "year": year,
        "format": fmt,
        "storage_path": str(storage_path) if storage_path else None,
        "dry_run": dry_run,
        "dataset_version": dataset_version or settings.dataset_version,
    }
    logger.info("Starting ingest command", extra={"context": context})

    pipeline.run(
        source,
        year=year,
        fmt=fmt,
        storage_path=storage_path,
        dry_run=dry_run,
        dataset_version=dataset_version,
    )

    logger.info("Finished ingest command", extra={"context": context})


if __name__ == "__main__":
    app()
