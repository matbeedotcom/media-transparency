"""API middleware for rate limiting and security.

Implements rate limiting using Redis for request throttling.
"""

import asyncio
import hashlib
import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from ..config import get_settings
from ..logging import get_context_logger
from . import RateLimitError

logger = get_context_logger(__name__)

# Rate limit configurations by endpoint category
RATE_LIMITS = {
    # Format: (requests, window_seconds)
    "default": (100, 60),  # 100 requests per minute
    "search": (30, 60),  # 30 searches per minute
    "detection": (10, 60),  # 10 detection requests per minute
    "ingestion": (5, 60),  # 5 ingestion triggers per minute
    "export": (10, 60),  # 10 exports per minute
    "auth": (20, 60),  # 20 auth attempts per minute
}

# Route patterns to rate limit categories
ROUTE_CATEGORIES = {
    "/api/v1/entities": "search",
    "/api/v1/relationships": "search",
    "/api/v1/detection": "detection",
    "/api/v1/ingestion": "ingestion",
    "/api/v1/reports": "export",
    "/api/v1/auth": "auth",
}


def get_rate_limit_category(path: str) -> str:
    """Determine rate limit category for a path.

    Args:
        path: Request path

    Returns:
        Rate limit category name
    """
    for pattern, category in ROUTE_CATEGORIES.items():
        if path.startswith(pattern):
            return category
    return "default"


def get_client_identifier(request: Request) -> str:
    """Get unique identifier for rate limiting.

    Uses user ID if authenticated, otherwise falls back to IP.

    Args:
        request: FastAPI request

    Returns:
        Client identifier string
    """
    # Prefer user ID if authenticated
    user_id = getattr(request.state, "user_id", None)
    if user_id:
        return f"user:{user_id}"

    # Fall back to IP address
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            ip = real_ip
        elif request.client:
            ip = request.client.host
        else:
            ip = "unknown"

    return f"ip:{ip}"


class InMemoryRateLimiter:
    """Simple in-memory rate limiter for development/fallback.

    Uses sliding window algorithm with token bucket.
    """

    def __init__(self):
        self._windows: dict[str, list[float]] = {}
        self._lock = asyncio.Lock()

    async def check_rate_limit(
        self, key: str, max_requests: int, window_seconds: int
    ) -> tuple[bool, int, int]:
        """Check if request is within rate limit.

        Args:
            key: Rate limit key (client + category)
            max_requests: Maximum requests in window
            window_seconds: Window size in seconds

        Returns:
            Tuple of (allowed, remaining, reset_seconds)
        """
        now = time.time()
        window_start = now - window_seconds

        async with self._lock:
            # Get or create window
            if key not in self._windows:
                self._windows[key] = []

            # Remove expired entries
            self._windows[key] = [
                ts for ts in self._windows[key] if ts > window_start
            ]

            # Check limit
            current_count = len(self._windows[key])
            remaining = max(0, max_requests - current_count - 1)

            if current_count >= max_requests:
                # Calculate reset time
                oldest = min(self._windows[key]) if self._windows[key] else now
                reset_seconds = int(oldest + window_seconds - now)
                return False, 0, max(1, reset_seconds)

            # Add this request
            self._windows[key].append(now)
            return True, remaining, window_seconds

    async def cleanup(self):
        """Remove expired entries to prevent memory growth."""
        now = time.time()
        max_window = max(limit[1] for limit in RATE_LIMITS.values())

        async with self._lock:
            keys_to_remove = []
            for key, timestamps in self._windows.items():
                self._windows[key] = [
                    ts for ts in timestamps if ts > now - max_window
                ]
                if not self._windows[key]:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self._windows[key]


class RedisRateLimiter:
    """Redis-based rate limiter for production.

    Uses sliding window algorithm with sorted sets.
    """

    def __init__(self, redis_client):
        self._redis = redis_client
        self._key_prefix = "mitds:ratelimit:"

    async def check_rate_limit(
        self, key: str, max_requests: int, window_seconds: int
    ) -> tuple[bool, int, int]:
        """Check if request is within rate limit using Redis.

        Args:
            key: Rate limit key (client + category)
            max_requests: Maximum requests in window
            window_seconds: Window size in seconds

        Returns:
            Tuple of (allowed, remaining, reset_seconds)
        """
        redis_key = f"{self._key_prefix}{key}"
        now = time.time()
        window_start = now - window_seconds

        try:
            # Use pipeline for atomic operations
            pipe = self._redis.pipeline()

            # Remove expired entries
            pipe.zremrangebyscore(redis_key, 0, window_start)

            # Count current entries
            pipe.zcard(redis_key)

            # Add new entry (will execute even if we later reject)
            member = f"{now}:{hashlib.md5(str(now).encode()).hexdigest()[:8]}"
            pipe.zadd(redis_key, {member: now})

            # Set expiry on key
            pipe.expire(redis_key, window_seconds + 1)

            results = await pipe.execute()
            current_count = results[1]

            if current_count >= max_requests:
                # Remove the entry we just added
                await self._redis.zrem(redis_key, member)

                # Get oldest entry for reset time
                oldest = await self._redis.zrange(redis_key, 0, 0, withscores=True)
                if oldest:
                    reset_seconds = int(oldest[0][1] + window_seconds - now)
                else:
                    reset_seconds = window_seconds

                return False, 0, max(1, reset_seconds)

            remaining = max(0, max_requests - current_count - 1)
            return True, remaining, window_seconds

        except Exception as e:
            logger.warning(f"Redis rate limit error: {e}, allowing request")
            # Fail open - allow request if Redis is unavailable
            return True, max_requests - 1, window_seconds


# Global rate limiter instance
_rate_limiter: InMemoryRateLimiter | RedisRateLimiter | None = None


async def get_rate_limiter() -> InMemoryRateLimiter | RedisRateLimiter:
    """Get or create rate limiter instance.

    Uses Redis in production, falls back to in-memory for development.
    """
    global _rate_limiter

    if _rate_limiter is not None:
        return _rate_limiter

    settings = get_settings()

    if settings.is_production:
        try:
            import redis.asyncio as redis

            client = redis.from_url(settings.redis_url)
            await client.ping()
            _rate_limiter = RedisRateLimiter(client)
            logger.info("Using Redis rate limiter")
        except Exception as e:
            logger.warning(f"Redis unavailable for rate limiting: {e}")
            _rate_limiter = InMemoryRateLimiter()
    else:
        _rate_limiter = InMemoryRateLimiter()
        logger.info("Using in-memory rate limiter")

    return _rate_limiter


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware for API rate limiting.

    Implements sliding window rate limiting with different limits
    for different endpoint categories.
    """

    # Paths to skip rate limiting
    SKIP_PATHS = {
        "/health",
        "/api/v1/health",
        "/docs",
        "/redoc",
        "/openapi.json",
    }

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Process request with rate limiting.

        Args:
            request: Incoming request
            call_next: Next middleware/handler

        Returns:
            Response from handler or 429 error
        """
        # Skip non-API and health endpoints
        if (
            request.url.path in self.SKIP_PATHS
            or not request.url.path.startswith("/api/")
        ):
            return await call_next(request)

        # Get client identifier and rate limit category
        client_id = get_client_identifier(request)
        category = get_rate_limit_category(request.url.path)
        max_requests, window_seconds = RATE_LIMITS.get(category, RATE_LIMITS["default"])

        # Create rate limit key
        rate_key = f"{client_id}:{category}"

        # Check rate limit
        limiter = await get_rate_limiter()
        allowed, remaining, reset_seconds = await limiter.check_rate_limit(
            rate_key, max_requests, window_seconds
        )

        if not allowed:
            logger.warning(
                "rate_limit_exceeded",
                client=client_id,
                category=category,
                path=request.url.path,
            )
            raise RateLimitError(retry_after=reset_seconds)

        # Call next handler
        response = await call_next(request)

        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_seconds)

        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware to add security headers to responses."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Add security headers to response.

        Args:
            request: Incoming request
            call_next: Next middleware/handler

        Returns:
            Response with security headers
        """
        response = await call_next(request)

        # Content Security Policy
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https:; "
            "font-src 'self' data:; "
            "connect-src 'self'"
        )

        # Other security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
            "magnetometer=(), microphone=(), payment=(), usb=()"
        )

        # HSTS (only in production with HTTPS)
        settings = get_settings()
        if settings.is_production:
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains; preload"
            )

        return response


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Middleware to add request ID for tracing."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Add request ID to request state and response headers.

        Args:
            request: Incoming request
            call_next: Next middleware/handler

        Returns:
            Response with request ID header
        """
        from uuid import uuid4

        # Check for existing request ID from client
        request_id = request.headers.get("X-Request-ID", str(uuid4()))

        # Store in request state for logging
        request.state.request_id = request_id

        # Call next handler
        response = await call_next(request)

        # Add to response headers
        response.headers["X-Request-ID"] = request_id

        return response


def setup_middleware(app) -> None:
    """Configure all middleware for the FastAPI application.

    Args:
        app: FastAPI application instance
    """
    from .audit import AuditMiddleware

    # Order matters: outermost middleware runs first
    # 1. Request ID (for tracing)
    app.add_middleware(RequestIdMiddleware)

    # 2. Security headers
    app.add_middleware(SecurityHeadersMiddleware)

    # 3. Rate limiting
    app.add_middleware(RateLimitMiddleware)

    # 4. Audit logging
    app.add_middleware(AuditMiddleware)

    logger.info("API middleware configured")
