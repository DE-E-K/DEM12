"""
data-generator/config.py
─────────────────────────
Minimal Pydantic settings used only by the data-generator container.
Mirrors the relevant subset of include/config.py so the generator
image stays lightweight (no psycopg2, no Airflow).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GeneratorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    minio_root_user:       str = Field(..., min_length=3)
    minio_root_password:   str = Field(..., min_length=8)
    minio_endpoint:        str = Field(...)
    minio_raw_bucket:      str = Field("raw-data")
    minio_processed_bucket: str = Field("processed-data")

    generator_num_rows: int = Field(500, ge=1, le=1_000_000)
    generator_seed:     int = Field(42)

    @field_validator("minio_endpoint")
    @classmethod
    def validate_endpoint(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("MINIO_ENDPOINT must start with http:// or https://")
        return v.rstrip("/")


@lru_cache(maxsize=1)
def get_settings() -> GeneratorSettings:
    return GeneratorSettings()


settings: GeneratorSettings = get_settings()
