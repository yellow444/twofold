"""Logging helpers for the quality validation agent."""

from __future__ import annotations

import logging
from typing import Iterable

_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def configure_logging(
    level: str | int = logging.INFO, *, handlers: Iterable[logging.Handler] | None = None
) -> None:
    """Configure the root logger used by the CLI entrypoints."""

    logging.basicConfig(level=level, format=_DEFAULT_FORMAT, handlers=list(handlers or []))


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger."""

    return logging.getLogger(name)
