from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agents.ingest.app.schemas import CANONICAL_ORDER
from agents.quality.app.checks import CheckStatus
from agents.quality.app.config import QualitySettings
from agents.quality.app.pipeline import QualityReport, run_pipeline
from agents.quality.app.repository import QualityRepository

SAMPLE_PATH = REPO_ROOT / "samples/rosaviation_sample.csv"


def _sample_dataframe() -> pd.DataFrame:
    raw = pd.read_csv(SAMPLE_PATH)
    raw["start_time_utc"] = pd.to_datetime(raw.pop("start_time"), utc=True)
    raw["end_time_utc"] = pd.to_datetime(raw.pop("end_time"), utc=True)
    raw["duration_minutes"] = raw["duration_minutes"].astype(float)
    raw["surrogate_id"] = None
    raw["superseded"] = False

    for column in CANONICAL_ORDER:
        if column not in raw.columns:
            if column == "superseded":
                raw[column] = False
            else:
                raw[column] = None

    return raw[CANONICAL_ORDER].copy()


def _make_settings(tmp_path: Path) -> QualitySettings:
    return QualitySettings.model_validate(
        {
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/testdb",
            "DATABASE_SCHEMA": "public",
            "TABLE_NAME": "normalized_flights",
            "ARTIFACTS_DIR": str(tmp_path),
        }
    )


def test_run_pipeline_success(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    dataframe = _sample_dataframe()

    def loader(_: QualitySettings, __: str | None) -> pd.DataFrame:
        return dataframe

    report: QualityReport = run_pipeline(
        settings,
        dataset_version="2024-01",
        loader=loader,
        dry_run=True,
    )

    assert report.status is CheckStatus.OK
    assert {check.status for check in report.checks} == {CheckStatus.OK}
    assert report.quality_status == "validated"
    assert report.warn_count == 0
    assert report.fail_count == 0


def test_run_pipeline_detects_failures(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    dataframe = _sample_dataframe()
    dataframe.loc[0, "duration_minutes"] = -5
    dataframe.loc[1, "latitude"] = 120
    dataframe = pd.concat([dataframe, dataframe.iloc[[0]]], ignore_index=True)

    def loader(_: QualitySettings, __: str | None) -> pd.DataFrame:
        return dataframe

    report = run_pipeline(
        settings,
        dataset_version="2024-02",
        loader=loader,
        dry_run=True,
    )

    statuses = {check.name: check.status for check in report.checks}
    assert statuses["duration_range"] is CheckStatus.FAIL
    assert statuses["coordinate_range"] is CheckStatus.FAIL
    assert statuses["uniqueness"] is CheckStatus.FAIL
    assert report.status is CheckStatus.FAIL
    assert report.quality_status == "quality_fail"
    assert report.fail_count >= 1


def test_monthly_completeness_warns_for_gaps(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    dataframe = _sample_dataframe().iloc[:2].copy()
    dataframe.loc[:, "start_time_utc"] = pd.to_datetime(
        [
            "2024-01-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
        ]
    )
    dataframe.loc[:, "end_time_utc"] = pd.to_datetime(
        [
            "2024-01-01T01:00:00Z",
            "2024-03-01T01:00:00Z",
        ]
    )

    def loader(_: QualitySettings, __: str | None) -> pd.DataFrame:
        return dataframe

    report = run_pipeline(
        settings,
        dataset_version="2024-gap",
        loader=loader,
        dry_run=True,
    )

    statuses = {check.name: check.status for check in report.checks}
    assert statuses["monthly_completeness"] is CheckStatus.WARN
    assert report.status is CheckStatus.WARN
    assert report.quality_status == "quality_warn"
    assert report.warn_count >= 1


def test_run_pipeline_persists_results(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    dataframe = _sample_dataframe()

    def loader(_: QualitySettings, __: str | None) -> pd.DataFrame:
        return dataframe

    repository = MagicMock(spec=QualityRepository)
    connection = MagicMock()
    repository.connection.return_value.__enter__.return_value = connection
    repository.fetch_dataset_version_id.return_value = 7

    report = run_pipeline(
        settings,
        dataset_version="2024-03",
        loader=loader,
        repository=repository,
    )

    repository.connection.assert_called_once()
    repository.fetch_dataset_version_id.assert_called_once_with(connection, "2024-03")
    repository.replace_quality_reports.assert_called_once()
    repository.update_dataset_version.assert_called_once_with(
        connection,
        7,
        status=report.quality_status,
        warn_count=report.warn_count,
        fail_count=report.fail_count,
    )
    assert report.status is CheckStatus.OK
