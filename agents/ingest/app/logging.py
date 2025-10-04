"""Logging helpers for the ingest application."""

from __future__ import annotations

import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

_LOGGING_CONFIGURED = False


def configure_logging(level: str | int = "INFO") -> None:
    """Configure application-wide logging with a Rich handler."""

    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=Console(stderr=True), rich_tracebacks=True)],
    )
    _LOGGING_CONFIGURED = True


def get_logger(name: Optional[str] = None, level: str | int = "INFO") -> logging.Logger:
    """Return a logger instance, configuring logging if necessary."""

    configure_logging(level)
    return logging.getLogger(name or "app")
