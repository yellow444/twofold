from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
import sys

import pytest
from psycopg.types.json import Jsonb

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agents.quality.app.checks import CheckStatus
from agents.quality.app.repository import QualityReportEntry, QualityRepository


def _make_connection() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection, cursor


def test_fetch_dataset_version_id_returns_value() -> None:
    repo = QualityRepository("postgresql://user:pass@localhost:5432/testdb")
    connection, cursor = _make_connection()
    cursor.fetchone.return_value = {"id": 42}

    version_id = repo.fetch_dataset_version_id(connection, "2024-Q1")

    assert version_id == 42
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    assert "FROM dataset_version" in sql
    assert params == ("2024-Q1",)


def test_fetch_dataset_version_id_missing_raises() -> None:
    repo = QualityRepository("postgresql://user:pass@localhost:5432/testdb")
    connection, cursor = _make_connection()
    cursor.fetchone.return_value = None

    with pytest.raises(ValueError):
        repo.fetch_dataset_version_id(connection, "missing")


def test_replace_quality_reports_inserts_entries() -> None:
    repo = QualityRepository("postgresql://user:pass@localhost:5432/testdb")
    connection, cursor = _make_connection()
    entry = QualityReportEntry(
        check_name="duration_range",
        severity=CheckStatus.FAIL,
        payload={"summary": "out of range", "impacted_records": ["A1"]},
    )

    repo.replace_quality_reports(connection, 5, [entry])

    assert cursor.execute.call_count == 2
    delete_call = cursor.execute.call_args_list[0]
    insert_call = cursor.execute.call_args_list[1]
    delete_sql, delete_params = delete_call[0]
    assert "DELETE FROM quality_report" in delete_sql
    assert delete_params == (5,)
    insert_sql, insert_params = insert_call[0]
    assert "INSERT INTO quality_report" in insert_sql
    assert insert_params[0] == 5
    assert insert_params[2] == "duration_range"
    assert insert_params[3] == "FAIL"
    assert isinstance(insert_params[4], Jsonb)


def test_update_dataset_version_sets_status() -> None:
    repo = QualityRepository("postgresql://user:pass@localhost:5432/testdb")
    connection, cursor = _make_connection()

    repo.update_dataset_version(connection, 8, status="quality_warn", warn_count=2, fail_count=1)

    # First two calls come from ensuring helper columns, final call performs UPDATE
    sql_statements = [call[0][0] for call in cursor.execute.call_args_list]
    assert any("ALTER TABLE dataset_version" in sql for sql in sql_statements[:2])
    assert any("UPDATE dataset_version" in sql for sql in sql_statements)
    _, update_params = cursor.execute.call_args_list[-1][0]
    assert update_params == ("quality_warn", 2, 1, 8)
