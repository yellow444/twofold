from __future__ import annotations

from pathlib import Path

import pandas as pd

from agents.ingest.app.schemas import CANONICAL_ORDER
from agents.quality.app.checks import CheckStatus
from agents.quality.app.config import QualitySettings
from agents.quality.app.pipeline import QualityReport, run_pipeline

REPO_ROOT = Path(__file__).resolve().parents[3]
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

    report: QualityReport = run_pipeline(settings, dataset_version="2024-01", loader=loader)

    assert report.status is CheckStatus.OK
    assert {check.status for check in report.checks} == {CheckStatus.OK}


def test_run_pipeline_detects_failures(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    dataframe = _sample_dataframe()
    dataframe.loc[0, "duration_minutes"] = -5
    dataframe.loc[1, "latitude"] = 120
    dataframe = pd.concat([dataframe, dataframe.iloc[[0]]], ignore_index=True)

    def loader(_: QualitySettings, __: str | None) -> pd.DataFrame:
        return dataframe

    report = run_pipeline(settings, dataset_version="2024-02", loader=loader)

    statuses = {check.name: check.status for check in report.checks}
    assert statuses["duration_range"] is CheckStatus.FAIL
    assert statuses["coordinate_range"] is CheckStatus.FAIL
    assert statuses["uniqueness"] is CheckStatus.FAIL
    assert report.status is CheckStatus.FAIL


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

    report = run_pipeline(settings, dataset_version="2024-gap", loader=loader)

    statuses = {check.name: check.status for check in report.checks}
    assert statuses["monthly_completeness"] is CheckStatus.WARN
    assert report.status is CheckStatus.WARN
