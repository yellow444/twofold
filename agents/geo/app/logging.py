"""Logging helpers for the geo agent."""

from __future__ import annotations

import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

_LOGGING_CONFIGURED = False


def configure_logging(level: str | int = "INFO") -> None:
    """Configure logging for the geo agent."""

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
    """Return a configured logger instance."""

    configure_logging(level)
    return logging.getLogger(name or "agents.geo")
