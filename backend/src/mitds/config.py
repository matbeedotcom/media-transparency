"""Configuration management for MITDS.

Uses pydantic-settings to load configuration from environment variables.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_env_file() -> Path | None:
    """Search for .env file in common locations."""
    # Check current working directory first
    cwd = Path.cwd()
    if (cwd / ".env").exists():
        return cwd / ".env"

    # Check parent directories (up to 5 levels) for project root .env
    check_dir = cwd
    for _ in range(5):
        if (check_dir / ".env").exists():
            return check_dir / ".env"
        parent = check_dir.parent
        if parent == check_dir:
            break
        check_dir = parent

    # Check relative to this config file (backend/src/mitds/config.py -> project root)
    config_path = Path(__file__).resolve()
    project_root = config_path.parent.parent.parent.parent  # Up to project root
    if (project_root / ".env").exists():
        return project_root / ".env"

    return None


# Find .env file location
_env_file = _find_env_file()


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=str(_env_file) if _env_file else ".env",
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
    ised_api_key: str = Field(default="", repr=False)
    meta_app_id: str = Field(default="", repr=False)
    meta_app_secret: str = Field(default="", repr=False)
    meta_access_token: str = Field(default="", repr=False)

    # Meta OAuth Configuration
    # NOTE: Meta requires HTTPS for callback URLs in production.
    # localhost is allowed without HTTPS during development.
    # Set these via environment variables for production:
    #   META_OAUTH_REDIRECT_URI=https://your-domain.com/api/v1/meta/auth/callback
    #   META_OAUTH_FRONTEND_REDIRECT=https://your-domain.com/settings
    meta_oauth_redirect_uri: str = Field(
        default="http://localhost:8000/api/v1/meta/auth/callback",
        description="OAuth callback URL for Meta/Facebook authentication (must be HTTPS in production)",
    )
    meta_oauth_frontend_redirect: str = Field(
        default="http://localhost:5173/settings",
        description="Frontend URL to redirect to after Meta OAuth",
    )

    # =========================
    # Provincial Registry Credentials
    # =========================
    # Ontario (MyOntario / ServiceOntario)
    ontario_registry_username: str = Field(default="", repr=False)
    ontario_registry_password: str = Field(default="", repr=False)

    # Saskatchewan (ISC)
    saskatchewan_registry_username: str = Field(default="", repr=False)
    saskatchewan_registry_password: str = Field(default="", repr=False)

    # Manitoba (Companies Office)
    manitoba_registry_username: str = Field(default="", repr=False)
    manitoba_registry_password: str = Field(default="", repr=False)

    # British Columbia (BC OnLine)
    bc_registry_username: str = Field(default="", repr=False)
    bc_registry_password: str = Field(default="", repr=False)

    # Yukon (Corporate Online)
    yukon_registry_username: str = Field(default="", repr=False)
    yukon_registry_password: str = Field(default="", repr=False)

    def get_registry_credentials(self, province: str) -> tuple[str, str] | None:
        """Get credentials for a provincial registry.

        Args:
            province: Province code (e.g., 'ON', 'SK')

        Returns:
            Tuple of (username, password) if configured, None otherwise
        """
        credentials_map = {
            "ON": (self.ontario_registry_username, self.ontario_registry_password),
            "SK": (self.saskatchewan_registry_username, self.saskatchewan_registry_password),
            "MB": (self.manitoba_registry_username, self.manitoba_registry_password),
            "BC": (self.bc_registry_username, self.bc_registry_password),
            "YT": (self.yukon_registry_username, self.yukon_registry_password),
        }
        creds = credentials_map.get(province.upper())
        if creds and creds[0] and creds[1]:
            return creds
        return None

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
