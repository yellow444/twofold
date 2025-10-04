from __future__ import annotations

from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pl = pytest.importorskip("polars")

from app.formats import detect_format, load_records

SAMPLES_DIR = Path(__file__).resolve().parents[2] / "samples"


def test_load_csv_sample() -> None:
    source = SAMPLES_DIR / "rosaviation_sample.csv"
    result = load_records(str(source))
    assert isinstance(result.records, pl.DataFrame)
    assert result.records.height > 0
    assert "flight_id" in result.records.columns


def test_load_excel(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "Flight ID": ["RU-1"],
            "Start Time": ["2024-01-01T00:00:00"],
            "End Time": ["2024-01-01T01:00:00"],
            "Duration (minutes)": ["60"],
            "Region Code": ["RU-MOW"],
        }
    )
    path = tmp_path / "sample.xlsx"
    frame.to_excel(path, index=False)
    fmt = detect_format(path)
    result = load_records(path, format_hint=fmt)
    assert "start_time" in result.records.columns
    assert result.records["duration_minutes"][0] == "60"


def test_load_pdf_with_fallback(tmp_path: Path) -> None:
    path = tmp_path / "sample.pdf"
    path.write_text(
        "%PDF-1.4\n"
        "1 0 obj<>\n"
        "flight_id,start_time,end_time,duration_minutes,region_code\n"
        "RU-1,2024-01-01T00:00:00,2024-01-01T00:30:00,30,RU-MOW\n"
        "%%EOF\n"
    )
    result = load_records(path)
    assert result.metadata.get("degraded") is True
    assert result.records.height == 1
    assert result.records["duration_minutes"][0] == "30"


def test_load_html(tmp_path: Path) -> None:
    path = tmp_path / "sample.html"
    path.write_text(
        """
        <html>
          <head>
            <meta name="report-tz" content="Europe/Moscow" />
          </head>
          <body>
            <table>
              <thead>
                <tr>
                  <th>Flight Id</th>
                  <th>Start Time</th>
                  <th>End Time</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>RU-1</td>
                  <td>2024-01-01T00:00</td>
                  <td>2024-01-01T01:00</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """
    )
    result = load_records(path)
    assert result.metadata["report_timezone"] == "Europe/Moscow"
    assert "flight_id" in result.records.columns
    assert result.records.height == 1
