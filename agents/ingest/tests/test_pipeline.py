from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Iterator

import boto3
import pytest
from moto import mock_aws
from psycopg.rows import dict_row
from typer.testing import CliRunner

from app.cli import app

SAMPLES_ROOT = Path(__file__).resolve().parents[3] / "samples"
SAMPLE_FILE = SAMPLES_ROOT / "rosaviation_sample.csv"

runner = CliRunner()


@pytest.fixture()
def aws_client() -> Iterator[boto3.client]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture()
def migrated_db(postgresql_proc, postgresql):  # type: ignore[no-untyped-def]
    migration_sql = (Path(__file__).resolve().parents[4] / "infra/postgres/migrations/0001_initial.sql").read_text()
    with postgresql.cursor() as cur:
        cur.execute(migration_sql)
    postgresql.commit()
    yield postgresql_proc, postgresql


@pytest.mark.usefixtures("aws_client")
@pytest.mark.skipif(shutil.which("pg_ctl") is None, reason="pg_ctl executable not available")
def test_full_pipeline_ingest(monkeypatch, migrated_db):  # type: ignore[no-untyped-def]
    proc, conn = migrated_db

    monkeypatch.setenv("POSTGRES_HOST", proc.host)
    monkeypatch.setenv("POSTGRES_PORT", str(proc.port))
    monkeypatch.setenv("POSTGRES_DB", proc.dbname)
    monkeypatch.setenv("POSTGRES_USER", proc.user)
    if proc.password:
        monkeypatch.setenv("POSTGRES_PASSWORD", proc.password)
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test-key")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test-secret")
    monkeypatch.setenv("MINIO_BUCKET", "ingest-test")
    monkeypatch.setenv("MINIO_ENDPOINT", "")
    monkeypatch.setenv("MINIO_SECURE", "true")
    monkeypatch.setenv("MINIO_REGION", "us-east-1")
    monkeypatch.setenv("STORAGE__DATASET_PREFIX", "datasets")

    dataset_version = "test-version"

    result = runner.invoke(
        app,
        [
            "ingest",
            str(SAMPLE_FILE),
            "--year",
            "2024",
            "--dataset-version",
            dataset_version,
        ],
    )
    assert result.exit_code == 0, result.output

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT id, version_name, status, checksum, artifacts FROM dataset_version")
        dataset_row = cur.fetchone()
        assert dataset_row is not None
        assert dataset_row["version_name"] == dataset_version
        assert dataset_row["status"] == "ingested"
        assert dataset_row["checksum"]
        artifacts = dataset_row["artifacts"]
        assert artifacts["raw"].endswith("raw.csv")
        assert artifacts["normalized"].endswith("normalized.parquet")
        assert artifacts["lineage"].endswith("lineage.json")

        cur.execute(
            "SELECT COUNT(*) FROM flights_raw WHERE dataset_version_id = %s",
            (dataset_row["id"],),
        )
        raw_count = cur.fetchone()[0]
        assert raw_count == 5

        cur.execute(
            "SELECT COUNT(*) FROM flights_norm WHERE dataset_version_id = %s",
            (dataset_row["id"],),
        )
        norm_count = cur.fetchone()[0]
        assert norm_count == 5

    s3_client = boto3.client("s3", region_name="us-east-1")
    objects = s3_client.list_objects_v2(Bucket="ingest-test")
    keys = {item["Key"] for item in objects.get("Contents", [])}
    expected_keys = {
        "datasets/2024/test-version/raw.csv",
        "datasets/2024/test-version/normalized.parquet",
        "datasets/2024/test-version/lineage.json",
    }
    assert expected_keys.issubset(keys)

    lineage_obj = s3_client.get_object(Bucket="ingest-test", Key="datasets/2024/test-version/lineage.json")
    lineage_payload = json.loads(lineage_obj["Body"].read().decode("utf-8"))
    assert lineage_payload["counts"]["raw"] == 5
    assert lineage_payload["counts"]["normalized"] == 5
    assert lineage_payload["artifacts"]["lineage"].endswith("lineage.json")
    assert lineage_payload["checksum"] == dataset_row["checksum"]
