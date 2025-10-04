"""Object storage helpers for uploading ingest artifacts."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import AppSettings, MinioSettings, StorageSettings


@dataclass(slots=True)
class StorageClient:
    """Simple S3/MinIO wrapper used by the ingest pipeline."""

    minio: MinioSettings
    storage: StorageSettings

    def __post_init__(self) -> None:
        session = boto3.session.Session()
        endpoint = self._normalize_endpoint(self.minio.endpoint)
        use_ssl = endpoint.startswith("https://") if endpoint else self.minio.secure
        self.client = session.client(
            "s3",
            endpoint_url=endpoint or None,
            aws_access_key_id=self.minio.access_key or "minio",
            aws_secret_access_key=self.minio.secret_key or "minio123",
            region_name=self.minio.region or "us-east-1",
            use_ssl=use_ssl,
            config=Config(signature_version="s3v4"),
        )
        self.bucket = self.minio.bucket or self.storage.dataset_bucket or "datasets"
        self.prefix = (self.storage.dataset_prefix or "datasets").strip("/")
        self._ensure_bucket()

    @classmethod
    def from_settings(cls, settings: AppSettings) -> "StorageClient":
        return cls(settings.minio, settings.storage)

    def _normalize_endpoint(self, endpoint: Optional[str]) -> str:
        if not endpoint:
            return ""
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        scheme = "https" if self.minio.secure else "http"
        return f"{scheme}://{endpoint}"

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            create_kwargs: Dict[str, Any] = {"Bucket": self.bucket}
            region = self.minio.region or "us-east-1"
            if region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            self.client.create_bucket(**create_kwargs)

    def build_object_key(
        self,
        year: int,
        version: str,
        filename: str,
        extension: str,
        *,
        prefix: Optional[str] = None,
    ) -> str:
        base_prefix = (prefix or self.prefix).strip("/")
        parts = [part for part in (base_prefix, str(year), version, f"{filename}.{extension}") if part]
        return "/".join(parts)

    def build_uri(
        self,
        year: int,
        version: str,
        filename: str,
        extension: str,
        *,
        prefix: Optional[str] = None,
    ) -> str:
        key = self.build_object_key(year, version, filename, extension, prefix=prefix)
        return f"s3://{self.bucket}/{key}"

    def upload_parquet(
        self,
        year: int,
        version: str,
        name: str,
        table: pa.Table | pl.DataFrame,
        *,
        prefix: Optional[str] = None,
    ) -> str:
        if isinstance(table, pl.DataFrame):
            arrow_table = table.to_arrow()
        else:
            arrow_table = table
        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        buffer.seek(0)
        key = self.build_object_key(year, version, name, "parquet", prefix=prefix)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/x-parquet",
        )
        return f"s3://{self.bucket}/{key}"

    def upload_csv(
        self,
        year: int,
        version: str,
        name: str,
        data: pa.Table | pl.DataFrame,
        *,
        prefix: Optional[str] = None,
    ) -> str:
        df = data if isinstance(data, pl.DataFrame) else pl.from_arrow(data)
        buffer = io.StringIO()
        df.write_csv(buffer, has_header=True)
        key = self.build_object_key(year, version, name, "csv", prefix=prefix)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        return f"s3://{self.bucket}/{key}"

    def upload_json(
        self,
        year: int,
        version: str,
        name: str,
        payload: Dict[str, Any],
        *,
        prefix: Optional[str] = None,
    ) -> str:
        key = self.build_object_key(year, version, name, "json", prefix=prefix)
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"
