"""Command-line interface for the geo agent."""

from __future__ import annotations

from typing import Optional

import psycopg
import typer

from .config import AppSettings
from .logging import configure_logging, get_logger
from .shapes import refresh_regions

app = typer.Typer(add_completion=False, no_args_is_help=True, rich_markup_mode="markdown")


@app.callback()
def init() -> None:
    """Initialize application services before command execution."""

    configure_logging()


@app.command(help="Load region shapes into the database.")
def load_shapes(
    source: Optional[str] = typer.Option(
        None,
        "--source",
        "-s",
        help="Path or URL to the RF subjects shapefile (overrides GEO_SHAPES_SOURCE).",
    ),
    database_dsn: Optional[str] = typer.Option(
        None,
        "--database-dsn",
        help="Database DSN for the Postgres instance (overrides GEO_DATABASE_DSN).",
    ),
) -> None:
    """Refresh the regions table using the provided shapefile source."""

    settings = AppSettings()
    shapes_source = source or settings.shapes_source
    if not shapes_source:
        raise typer.BadParameter(
            "No shapes source provided. Specify --source or configure GEO_SHAPES_SOURCE.",
            param_hint="--source",
        )

    dsn = database_dsn or settings.database_dsn
    if not dsn:
        raise typer.BadParameter(
            "No database DSN provided. Specify --database-dsn or configure GEO_DATABASE_DSN.",
            param_hint="--database-dsn",
        )

    logger = get_logger("agents.geo.cli")
    logger.info("Refreshing regions table", extra={"context": {"source": shapes_source}})

    with psycopg.connect(dsn) as conn:
        count = refresh_regions(conn, shapes_source)
        logger.info("Loaded %s regions", count)


if __name__ == "__main__":
    app()
