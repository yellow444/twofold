"""HTML table loader using BeautifulSoup and lxml."""

from __future__ import annotations

from pathlib import Path
from typing import Any, List

from bs4 import BeautifulSoup
import polars as pl

from . import FormatHandler, FormatReadResult, register_handler
from ._common import dataframe_from_rows, sanitize

_HTML_SUFFIXES = {".html", ".htm"}


def _detect(source: str | Path) -> bool:
    return Path(str(source)).suffix.lower() in _HTML_SUFFIXES


def _extract_table(table: Any) -> pl.DataFrame:
    headers: List[str] = []
    data_rows: List[List[str]] = []

    thead = table.find("thead")
    if thead:
        header_row = thead.find_all("tr")[-1]
        headers = [cell.get_text(strip=True) for cell in header_row.find_all(["th", "td"])]
        tbody_rows = table.find_all("tbody")
        if tbody_rows:
            for body in tbody_rows:
                for tr in body.find_all("tr"):
                    cells = [cell.get_text(strip=True) for cell in tr.find_all(["td", "th"])]
                    if cells:
                        data_rows.append(cells)
    else:
        for idx, tr in enumerate(table.find_all("tr")):
            cells = [cell.get_text(strip=True) for cell in tr.find_all(["td", "th"])]
            if not cells:
                continue
            if not headers:
                headers = cells
                continue
            data_rows.append(cells)

    if not headers:
        return pl.DataFrame()

    df = dataframe_from_rows(headers, data_rows)
    return sanitize(df)


def _load(source: str | Path) -> FormatReadResult:
    path = Path(str(source))
    raw_html = path.read_text(encoding="utf-8")
    soup = BeautifulSoup(raw_html, "lxml")
    metadata: dict[str, Any] = {"source_path": str(path)}
    tz_meta = soup.find("meta", attrs={"name": "report-tz"})
    if tz_meta and tz_meta.get("content"):
        metadata["report_timezone"] = tz_meta["content"].strip()

    table = soup.find("table")
    if table is None:
        raise ValueError(f"No <table> found in HTML source: {source}")

    df = _extract_table(table)
    return FormatReadResult(records=df, metadata=metadata)


register_handler(
    FormatHandler(
        name="html",
        aliases=("htm", "text/html"),
        loader=_load,
        detector=_detect,
    )
)
