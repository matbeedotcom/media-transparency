"""Request validation and sanitization utilities.

Provides input sanitization to prevent injection attacks and
ensure data integrity.
"""

import html
import re
from typing import Any
from uuid import UUID

from pydantic import BaseModel, field_validator, ConfigDict

from ..logging import get_context_logger

logger = get_context_logger(__name__)


# =========================
# Sanitization Functions
# =========================


def sanitize_string(value: str, max_length: int = 1000) -> str:
    """Sanitize a string input.

    - Removes null bytes
    - Strips leading/trailing whitespace
    - Truncates to max length
    - Escapes HTML entities

    Args:
        value: Input string
        max_length: Maximum allowed length

    Returns:
        Sanitized string
    """
    if not value:
        return value

    # Remove null bytes
    value = value.replace("\x00", "")

    # Strip whitespace
    value = value.strip()

    # Truncate
    if len(value) > max_length:
        value = value[:max_length]

    return value


def sanitize_html(value: str) -> str:
    """Escape HTML entities in a string.

    Args:
        value: Input string

    Returns:
        HTML-escaped string
    """
    return html.escape(value) if value else value


def sanitize_for_sql_like(value: str) -> str:
    """Escape special characters for SQL LIKE patterns.

    Escapes %, _, and \ to prevent SQL injection in LIKE clauses.

    Args:
        value: Input string for LIKE pattern

    Returns:
        Escaped string safe for LIKE
    """
    if not value:
        return value

    # Escape backslash first, then special LIKE chars
    value = value.replace("\\", "\\\\")
    value = value.replace("%", "\\%")
    value = value.replace("_", "\\_")

    return value


def sanitize_for_cypher(value: str) -> str:
    """Escape special characters for Neo4j Cypher queries.

    Args:
        value: Input string for Cypher

    Returns:
        Escaped string safe for Cypher
    """
    if not value:
        return value

    # Escape backslashes and single quotes
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "\\'")
    value = value.replace('"', '\\"')

    return value


def sanitize_filename(value: str) -> str:
    """Sanitize a filename to prevent path traversal.

    Args:
        value: Input filename

    Returns:
        Safe filename
    """
    if not value:
        return value

    # Remove path separators and traversal
    value = value.replace("/", "")
    value = value.replace("\\", "")
    value = value.replace("..", "")

    # Remove null bytes
    value = value.replace("\x00", "")

    # Keep only safe characters
    value = re.sub(r"[^a-zA-Z0-9._-]", "_", value)

    # Prevent hidden files
    value = value.lstrip(".")

    return value[:255] if value else "unnamed"


def sanitize_email(value: str) -> str | None:
    """Validate and sanitize an email address.

    Args:
        value: Input email

    Returns:
        Sanitized email or None if invalid
    """
    if not value:
        return None

    value = value.strip().lower()

    # Basic email pattern
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, value):
        return None

    return value


def sanitize_url(value: str) -> str | None:
    """Validate and sanitize a URL.

    Args:
        value: Input URL

    Returns:
        Sanitized URL or None if invalid
    """
    if not value:
        return None

    value = value.strip()

    # Only allow http and https
    if not value.startswith(("http://", "https://")):
        return None

    # Basic URL pattern
    pattern = r"^https?://[a-zA-Z0-9][-a-zA-Z0-9]*(\.[a-zA-Z0-9][-a-zA-Z0-9]*)*(:\d+)?(/.*)?$"
    if not re.match(pattern, value):
        return None

    return value[:2048]


def validate_uuid(value: str) -> UUID | None:
    """Validate and parse a UUID string.

    Args:
        value: Input UUID string

    Returns:
        UUID object or None if invalid
    """
    if not value:
        return None

    try:
        return UUID(value.strip())
    except (ValueError, AttributeError):
        return None


def validate_positive_int(value: Any, max_value: int = 10000) -> int | None:
    """Validate a positive integer.

    Args:
        value: Input value
        max_value: Maximum allowed value

    Returns:
        Integer or None if invalid
    """
    try:
        num = int(value)
        if num < 0:
            return None
        if num > max_value:
            return max_value
        return num
    except (ValueError, TypeError):
        return None


# =========================
# Pydantic Validators
# =========================


class SanitizedStr(str):
    """String type that auto-sanitizes on creation."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, str):
            return cls(sanitize_string(v))
        raise TypeError("string required")


# =========================
# Request Models with Sanitization
# =========================


class SearchQueryParams(BaseModel):
    """Sanitized search query parameters."""

    model_config = ConfigDict(str_strip_whitespace=True)

    q: str | None = None
    entity_type: str | None = None
    jurisdiction: str | None = None
    limit: int = 20
    offset: int = 0

    @field_validator("q")
    @classmethod
    def sanitize_query(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return sanitize_string(v, max_length=500)

    @field_validator("entity_type", "jurisdiction")
    @classmethod
    def sanitize_filter(cls, v: str | None) -> str | None:
        if v is None:
            return None
        # Restrict to alphanumeric and underscore
        sanitized = re.sub(r"[^a-zA-Z0-9_-]", "", v)
        return sanitized[:50] if sanitized else None

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1:
            return 1
        if v > 100:
            return 100
        return v

    @field_validator("offset")
    @classmethod
    def validate_offset(cls, v: int) -> int:
        if v < 0:
            return 0
        if v > 10000:
            return 10000
        return v


class EntityIdParams(BaseModel):
    """Validated entity ID parameter."""

    entity_id: UUID

    @field_validator("entity_id", mode="before")
    @classmethod
    def validate_entity_id(cls, v):
        if isinstance(v, str):
            parsed = validate_uuid(v)
            if parsed is None:
                raise ValueError("Invalid UUID format")
            return parsed
        return v


class DateRangeParams(BaseModel):
    """Validated date range parameters."""

    model_config = ConfigDict(str_strip_whitespace=True)

    start_date: str | None = None
    end_date: str | None = None

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date(cls, v: str | None) -> str | None:
        if v is None:
            return None

        # Validate ISO date format
        pattern = r"^\d{4}-\d{2}-\d{2}$"
        if not re.match(pattern, v):
            raise ValueError("Date must be in YYYY-MM-DD format")

        return v


class ReportRequestParams(BaseModel):
    """Sanitized report request parameters."""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: str
    entity_ids: list[UUID] = []
    include_relationships: bool = True
    include_detection_scores: bool = True
    format: str = "json"

    @field_validator("title")
    @classmethod
    def sanitize_title(cls, v: str) -> str:
        sanitized = sanitize_string(v, max_length=200)
        if not sanitized:
            raise ValueError("Title is required")
        return sanitized

    @field_validator("entity_ids")
    @classmethod
    def validate_entity_ids(cls, v: list) -> list[UUID]:
        if len(v) > 100:
            raise ValueError("Maximum 100 entities per report")
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        allowed = {"json", "csv", "pdf"}
        if v.lower() not in allowed:
            raise ValueError(f"Format must be one of: {', '.join(allowed)}")
        return v.lower()


# =========================
# Injection Detection
# =========================


# Patterns that may indicate injection attempts
SUSPICIOUS_PATTERNS = [
    r"<script",  # XSS
    r"javascript:",  # XSS
    r"on\w+\s*=",  # Event handlers
    r";\s*(drop|delete|truncate|update|insert)\s",  # SQL
    r"'\s*(or|and)\s+\d+\s*=\s*\d+",  # SQL injection
    r"\$\{.*\}",  # Template injection
    r"{{.*}}",  # Template injection
    r"\\x[0-9a-fA-F]{2}",  # Encoded bytes
]


def detect_injection_attempt(value: str) -> bool:
    """Check if input contains suspicious patterns.

    Args:
        value: Input to check

    Returns:
        True if suspicious patterns found
    """
    if not value:
        return False

    value_lower = value.lower()

    for pattern in SUSPICIOUS_PATTERNS:
        if re.search(pattern, value_lower, re.IGNORECASE):
            logger.warning(
                "injection_attempt_detected",
                pattern=pattern,
                value_preview=value[:100],
            )
            return True

    return False


def sanitize_dict_values(
    data: dict[str, Any], max_depth: int = 5
) -> dict[str, Any]:
    """Recursively sanitize all string values in a dictionary.

    Args:
        data: Input dictionary
        max_depth: Maximum recursion depth

    Returns:
        Sanitized dictionary
    """
    if max_depth <= 0:
        return data

    result = {}
    for key, value in data.items():
        # Sanitize key
        key = sanitize_string(str(key), max_length=100)

        if isinstance(value, str):
            result[key] = sanitize_string(value)
        elif isinstance(value, dict):
            result[key] = sanitize_dict_values(value, max_depth - 1)
        elif isinstance(value, list):
            result[key] = [
                sanitize_string(v) if isinstance(v, str)
                else sanitize_dict_values(v, max_depth - 1) if isinstance(v, dict)
                else v
                for v in value
            ]
        else:
            result[key] = value

    return result
