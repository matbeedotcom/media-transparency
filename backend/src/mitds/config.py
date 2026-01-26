"""Configuration management for MITDS.

Uses pydantic-settings to load configuration from environment variables.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # =========================
    # Environment
    # =========================
    environment: Literal["development", "staging", "production"] = "development"

    # =========================
    # API Settings
    # =========================
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_debug: bool = False
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    # =========================
    # PostgreSQL
    # =========================
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "mitds"
    postgres_user: str = "mitds"
    postgres_password: str = Field(default="", repr=False)

    @computed_field
    @property
    def database_url(self) -> str:
        """SQLAlchemy database URL for PostgreSQL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field
    @property
    def database_url_sync(self) -> str:
        """Synchronous database URL for Alembic."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # =========================
    # Neo4j
    # =========================
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = Field(default="", repr=False)

    # =========================
    # Redis
    # =========================
    redis_url: str = "redis://localhost:6379/0"

    # =========================
    # S3/MinIO
    # =========================
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = Field(default="minioadmin", repr=False)
    s3_secret_key: str = Field(default="minioadmin", repr=False)
    s3_bucket: str = "mitds-raw"
    s3_region: str = "us-east-1"

    # =========================
    # Celery
    # =========================
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"

    # =========================
    # Data Source API Keys
    # =========================
    opencorporates_api_key: str = Field(default="", repr=False)
    meta_app_id: str = Field(default="", repr=False)
    meta_app_secret: str = Field(default="", repr=False)
    meta_access_token: str = Field(default="", repr=False)

    # =========================
    # JWT/Auth
    # =========================
    jwt_secret: str = Field(default="change-me-in-production", repr=False)
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24

    # =========================
    # Logging
    # =========================
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"

    # =========================
    # Feature Flags
    # =========================
    enable_meta_ads_ingestion: bool = False
    enable_opencorporates_ingestion: bool = False

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins as a list."""
        return [origin.strip() for origin in self.cors_origins.split(",")]

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment == "production"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
