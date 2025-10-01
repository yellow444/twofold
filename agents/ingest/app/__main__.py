"""Entry point for running the ingest CLI as a module."""

from .cli import app


def main() -> None:
    """Execute the Typer application."""

    app()


if __name__ == "__main__":
    main()
