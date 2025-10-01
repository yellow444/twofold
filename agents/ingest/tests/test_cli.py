from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import logging

from typer.testing import CliRunner

from app.cli import app
from app.pipeline import IngestPipeline


runner = CliRunner()


def test_help_output() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "ingest" in result.output


def test_ingest_command_invokes_pipeline(monkeypatch, caplog) -> None:  # type: ignore[no-untyped-def]
    captured: Dict[str, Any] = {}

    def fake_run(
        self: IngestPipeline,
        source: str,
        *,
        year: int | None,
        fmt: str | None,
        storage_path: Path | None,
        dry_run: bool,
        dataset_version: str | None,
    ) -> None:
        captured.update(
            {
                "source": source,
                "year": year,
                "format": fmt,
                "storage_path": storage_path,
                "dry_run": dry_run,
                "dataset_version": dataset_version,
            }
        )

    monkeypatch.setattr(IngestPipeline, "run", fake_run)

    caplog.set_level(logging.INFO)

    result = runner.invoke(
        app,
        [
            "ingest",
            "rosaviatsia",
            "--year",
            "2022",
            "--format",
            "csv",
            "--storage-path",
            "/tmp/data",
            "--dry-run",
            "--dataset-version",
            "v1",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "source": "rosaviatsia",
        "year": 2022,
        "format": "csv",
        "storage_path": Path("/tmp/data"),
        "dry_run": True,
        "dataset_version": "v1",
    }

    messages = [record.getMessage() for record in caplog.records]
    assert "Starting ingest command" in messages
    assert "Finished ingest command" in messages
