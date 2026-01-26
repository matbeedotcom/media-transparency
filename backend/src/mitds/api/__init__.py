"""FastAPI routes and API modules for MITDS.

Provides common response models, error handlers, and utilities.
"""

from typing import Any, Generic, TypeVar
from uuid import UUID

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

T = TypeVar("T")


# =========================
# Response Models
# =========================


class APIResponse(BaseModel, Generic[T]):
    """Standard API response wrapper."""

    success: bool = True
    data: T | None = None
    error: str | None = None


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response for list endpoints."""

    results: list[T]
    total: int
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        """Check if there are more results."""
        return self.offset + len(self.results) < self.total


class ErrorDetail(BaseModel):
    """Detailed error information."""

    code: str
    message: str
    field: str | None = None
    details: dict[str, Any] | None = None


class ErrorResponse(BaseModel):
    """Standard error response."""

    success: bool = False
    error: str
    error_code: str
    details: list[ErrorDetail] | None = None


# =========================
# Exception Classes
# =========================


class APIError(HTTPException):
    """Base API error with structured response."""

    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        details: list[ErrorDetail] | None = None,
    ):
        self.error_code = error_code
        self.message = message
        self.details = details
        super().__init__(status_code=status_code, detail=message)


class NotFoundError(APIError):
    """Resource not found error."""

    def __init__(self, resource: str, identifier: str | UUID):
        super().__init__(
            status_code=404,
            error_code="NOT_FOUND",
            message=f"{resource} not found: {identifier}",
        )


class ValidationError(APIError):
    """Request validation error."""

    def __init__(self, message: str, details: list[ErrorDetail] | None = None):
        super().__init__(
            status_code=422,
            error_code="VALIDATION_ERROR",
            message=message,
            details=details,
        )


class AuthenticationError(APIError):
    """Authentication required error."""

    def __init__(self, message: str = "Authentication required"):
        super().__init__(
            status_code=401,
            error_code="AUTHENTICATION_REQUIRED",
            message=message,
        )


class AuthorizationError(APIError):
    """Authorization denied error."""

    def __init__(self, message: str = "Access denied"):
        super().__init__(
            status_code=403,
            error_code="ACCESS_DENIED",
            message=message,
        )


class RateLimitError(APIError):
    """Rate limit exceeded error."""

    def __init__(self, retry_after: int = 60):
        super().__init__(
            status_code=429,
            error_code="RATE_LIMIT_EXCEEDED",
            message=f"Rate limit exceeded. Retry after {retry_after} seconds.",
        )
        self.retry_after = retry_after


# =========================
# Exception Handlers
# =========================


async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    """Handle APIError exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.message,
            error_code=exc.error_code,
            details=exc.details,
        ).model_dump(),
        headers={"X-Error-Code": exc.error_code},
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle generic HTTPException."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=str(exc.detail),
            error_code="HTTP_ERROR",
        ).model_dump(),
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected exceptions."""
    from mitds.logging import get_logger

    logger = get_logger(__name__)
    logger.exception(f"Unhandled exception: {exc}")

    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="An unexpected error occurred",
            error_code="INTERNAL_ERROR",
        ).model_dump(),
    )


def register_exception_handlers(app):
    """Register exception handlers with the FastAPI app."""
    app.add_exception_handler(APIError, api_error_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
