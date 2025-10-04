"""Application configuration models loaded from environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Connection parameters for the Postgres database."""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    sslmode: Optional[str] = None

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")


class MinioSettings(BaseSettings):
    """Configuration for connecting to the MinIO/S3-compatible storage."""

    endpoint: str = "localhost:9000"
    access_key: str = ""
    secret_key: str = ""
    region: Optional[str] = None
    bucket: Optional[str] = None
    secure: bool = False

    model_config = SettingsConfigDict(env_prefix="MINIO_", extra="ignore")


class StorageSettings(BaseSettings):
    """Dataset storage configuration used by the ingest pipeline."""

    dataset_root: Optional[Path] = Field(
        default=None,
        validation_alias=AliasChoices("DATASET_ROOT", "STORAGE__DATASET_ROOT"),
    )
    dataset_bucket: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DATASET_BUCKET", "STORAGE__DATASET_BUCKET"),
    )
    dataset_version: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DATASET_VERSION", "STORAGE__DATASET_VERSION"),
    )
    dataset_prefix: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DATASET_PREFIX", "STORAGE__DATASET_PREFIX"),
    )
    default_format: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DATASET_FORMAT", "STORAGE__DEFAULT_FORMAT"),
    )

    model_config = SettingsConfigDict(extra="ignore")


class AppSettings(BaseSettings):
    """Top-level application configuration container."""

    environment: str = Field(default="development", validation_alias=AliasChoices("ENVIRONMENT"))
    default_year: Optional[int] = Field(default=None, validation_alias=AliasChoices("DEFAULT_YEAR"))
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)

    model_config = SettingsConfigDict(env_nested_delimiter="__", extra="ignore")

    @property
    def dataset_root(self) -> Optional[Path]:
        """Shortcut to the configured dataset root path."""

        return self.storage.dataset_root

    @property
    def dataset_version(self) -> Optional[str]:
        """Return the default dataset version if defined."""

        return self.storage.dataset_version
