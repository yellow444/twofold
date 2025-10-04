from __future__ import annotations

from pathlib import Path
import shutil
import sys

import psycopg
import pytest
from shapely import wkt
from typer.testing import CliRunner

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.geo.app.cli import app

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture()
def shapes_source() -> Path:
    return DATA_DIR / "rf_subjects.zip"


def _prepare_schema(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS regions (
                    id SERIAL PRIMARY KEY,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    boundary GEOMETRY(MultiPolygon, 4326) NOT NULL
                )
                """
            )


@pytest.mark.skipif(shutil.which("pg_ctl") is None, reason="PostgreSQL server binaries not available")
def test_load_shapes_populates_regions(postgresql, shapes_source: Path) -> None:
    runner = CliRunner()
    dsn = postgresql.info.dsn

    _prepare_schema(dsn)

    result = runner.invoke(
        app,
        [
            "load-shapes",
            "--source",
            str(shapes_source),
            "--database-dsn",
            dsn,
        ],
    )
    assert result.exit_code == 0, result.stdout

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT code, name, ST_SRID(boundary), ST_AsText(boundary) FROM regions ORDER BY code"
            )
            rows = cur.fetchall()

    assert [(row[0], row[1]) for row in rows] == [("77", "Region 77"), ("78", "Region 78")]
    assert all(row[2] == 4326 for row in rows)

    multipolygon_types = [wkt.loads(row[3]).geom_type for row in rows]
    assert multipolygon_types == ["MultiPolygon", "MultiPolygon"]
