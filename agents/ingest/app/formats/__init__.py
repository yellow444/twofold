"""Source format registry and dispatch utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional

import polars as pl
import pyarrow as pa

logger = logging.getLogger("app.formats")


@dataclass(slots=True)
class FormatReadResult:
    """Container returned by loaders with records and auxiliary metadata."""

    records: pl.DataFrame | pa.Table
    metadata: Dict[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:  # pragma: no cover - defensive
        if self.metadata is None:
            object.__setattr__(self, "metadata", {})


LoaderFn = Callable[[str | Path], FormatReadResult]
DetectorFn = Callable[[str | Path], bool]


@dataclass(frozen=True, slots=True)
class FormatHandler:
    """Format handler definition in the registry."""

    name: str
    aliases: tuple[str, ...]
    loader: LoaderFn
    detector: Optional[DetectorFn] = None

    def matches(self, format_name: str) -> bool:
        normalized = format_name.lower()
        return normalized == self.name or normalized in self.aliases


_REGISTRY: Dict[str, FormatHandler] = {}
_ORDER: list[FormatHandler] = []


def register_handler(handler: FormatHandler) -> None:
    """Register a new format handler in the registry."""

    if handler.name in _REGISTRY:
        raise ValueError(f"Handler for format '{handler.name}' already registered")
    _REGISTRY[handler.name] = handler
    for alias in handler.aliases:
        if alias in _REGISTRY:
            logger.warning("Alias %s already registered, overriding", alias)
        _REGISTRY[alias] = handler
    _ORDER.append(handler)


def iter_handlers() -> Iterable[FormatHandler]:
    """Iterate over handlers in registration order."""

    return tuple(_ORDER)


def detect_format(source: str | Path, format_hint: str | None = None) -> str:
    """Detect source format using explicit hint, extension and registered detectors."""

    if format_hint:
        hint = format_hint.lower()
        handler = _REGISTRY.get(hint)
        if handler:
            return handler.name
        logger.warning("Unknown format hint '%s', falling back to auto detection", format_hint)

    path = Path(str(source))
    suffix = path.suffix.lower().lstrip(".")
    if suffix:
        handler = _REGISTRY.get(suffix)
        if handler:
            return handler.name
        # handle compound extensions like .xlsm
        for registered_suffix, registered_handler in _REGISTRY.items():
            if registered_suffix.startswith(suffix):
                return registered_handler.name

    for handler in iter_handlers():
        if handler.detector and handler.detector(source):
            return handler.name

    raise ValueError(f"Unable to detect format for source: {source}")


def load_records(source: str | Path, format_hint: str | None = None) -> FormatReadResult:
    """Load records from the provided source by dispatching to a registered handler."""

    fmt = detect_format(source, format_hint=format_hint)
    handler = _REGISTRY[fmt]
    logger.info("Loading records", extra={"context": {"source": str(source), "format": fmt}})
    return handler.loader(source)


# Register built-in handlers.
from . import csv as csv_handler  # noqa: E402  (registration side effects)
from . import excel as excel_handler  # noqa: E402
from . import html as html_handler  # noqa: E402
from . import pdf as pdf_handler  # noqa: E402

__all__ = [
    "FormatReadResult",
    "FormatHandler",
    "detect_format",
    "iter_handlers",
    "load_records",
    "register_handler",
]
