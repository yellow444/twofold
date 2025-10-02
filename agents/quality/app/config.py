"""Configuration for the quality validation agent."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import DirectoryPath, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class QualitySettings(BaseSettings):
    """Runtime configuration driven by environment variables."""

    database_url: Annotated[PostgresDsn, Field(alias="DATABASE_URL")]
    database_schema: str = Field(default="public", alias="DATABASE_SCHEMA")
    table_name: str = Field(default="normalized_flights", alias="TABLE_NAME")
    artifacts_dir: DirectoryPath | Path = Field(
        default_factory=lambda: Path("artifacts"), alias="ARTIFACTS_DIR"
    )
    default_dataset_version: str | None = Field(default=None, alias="DEFAULT_DATASET_VERSION")

    model_config = SettingsConfigDict(env_prefix="QUALITY_", env_file=None, extra="ignore")

    @property
    def resolved_artifacts_dir(self) -> Path:
        """Return a writable artifacts directory, creating it if necessary."""

        path = Path(self.artifacts_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path


def get_settings() -> QualitySettings:
    """Load :class:`QualitySettings` using the default environment lookup."""

    return QualitySettings()  # type: ignore[arg-type]
