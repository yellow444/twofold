"""Utility helpers shared across ingest components."""

from __future__ import annotations

import re
from typing import Iterable


_HEADER_CLEAN_RE = re.compile(r"[^0-9a-zA-Z]+")


def normalize_header(value: str) -> str:
    """Normalize column headers to snake_case for consistent matching."""

    simplified = _HEADER_CLEAN_RE.sub("_", value.strip().lower())
    simplified = simplified.strip("_")
    simplified = re.sub(r"__+", "_", simplified)
    return simplified


def ensure_unique(names: Iterable[str]) -> list[str]:
    """Ensure that the provided iterable of names is unique by suffixing duplicates."""

    result: list[str] = []
    seen: dict[str, int] = {}
    for name in names:
        if name not in seen:
            seen[name] = 0
            result.append(name)
            continue
        seen[name] += 1
        unique_name = f"{name}_{seen[name]}"
        while unique_name in seen:
            seen[name] += 1
            unique_name = f"{name}_{seen[name]}"
        seen[unique_name] = 0
        result.append(unique_name)
    return result
