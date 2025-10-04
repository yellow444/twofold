"""Application configuration for the geo agent."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Settings for the geo agent."""

    database_dsn: str
    shapes_source: Optional[str] = None

    model_config = SettingsConfigDict(env_prefix="GEO_", env_file=".env", env_file_encoding="utf-8")

    @field_validator("shapes_source")
    @classmethod
    def _validate_source(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        # Allow URLs and local paths
        if value.startswith("http://") or value.startswith("https://"):
            return value
        path = Path(value)
        if not path.exists():
            raise ValueError(f"Configured shapes source does not exist: {value}")
        return str(path)
