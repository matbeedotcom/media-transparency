"""Authentication and authorization middleware for MITDS.

Provides JWT-based authentication and role-based access control.
"""

from datetime import datetime, timedelta
from typing import Annotated

import jwt
from fastapi import Depends, Header, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from ..config import get_settings
from . import AuthenticationError, AuthorizationError

# Security scheme
security = HTTPBearer(auto_error=False)


# =========================
# User Models
# =========================


class User(BaseModel):
    """Authenticated user information."""

    id: str
    email: str
    name: str
    roles: list[str] = []

    def has_role(self, role: str) -> bool:
        """Check if user has a specific role."""
        return role in self.roles

    def is_admin(self) -> bool:
        """Check if user is an admin."""
        return "admin" in self.roles

    def is_analyst(self) -> bool:
        """Check if user is an analyst."""
        return "analyst" in self.roles or self.is_admin()


class TokenPayload(BaseModel):
    """JWT token payload."""

    sub: str  # Subject (user ID)
    email: str
    name: str
    roles: list[str] = []
    exp: datetime  # Expiration time
    iat: datetime  # Issued at time


# =========================
# JWT Functions
# =========================


def create_access_token(user: User) -> str:
    """Create a JWT access token for a user.

    Args:
        user: User to create token for

    Returns:
        Encoded JWT token
    """
    settings = get_settings()

    now = datetime.utcnow()
    payload = TokenPayload(
        sub=user.id,
        email=user.email,
        name=user.name,
        roles=user.roles,
        exp=now + timedelta(hours=settings.jwt_expiration_hours),
        iat=now,
    )

    return jwt.encode(
        payload.model_dump(),
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


def decode_access_token(token: str) -> TokenPayload:
    """Decode and validate a JWT access token.

    Args:
        token: JWT token to decode

    Returns:
        Token payload

    Raises:
        AuthenticationError: If token is invalid or expired
    """
    settings = get_settings()

    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
        return TokenPayload(**payload)
    except jwt.ExpiredSignatureError:
        raise AuthenticationError("Token has expired")
    except jwt.InvalidTokenError as e:
        raise AuthenticationError(f"Invalid token: {e}")


# =========================
# Dependency Injection
# =========================


async def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> User | None:
    """Get the current user if authenticated, otherwise None.

    Use this dependency when authentication is optional.
    """
    if not credentials:
        return None

    try:
        payload = decode_access_token(credentials.credentials)
        return User(
            id=payload.sub,
            email=payload.email,
            name=payload.name,
            roles=payload.roles,
        )
    except AuthenticationError:
        return None


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> User:
    """Get the current authenticated user.

    Use this dependency when authentication is required.

    Raises:
        AuthenticationError: If not authenticated
    """
    if not credentials:
        raise AuthenticationError("Authentication required")

    payload = decode_access_token(credentials.credentials)
    return User(
        id=payload.sub,
        email=payload.email,
        name=payload.name,
        roles=payload.roles,
    )


def require_role(role: str):
    """Create a dependency that requires a specific role.

    Usage:
        @router.get("/admin", dependencies=[Depends(require_role("admin"))])
        async def admin_endpoint():
            ...
    """

    async def role_checker(user: User = Depends(get_current_user)) -> User:
        if not user.has_role(role):
            raise AuthorizationError(f"Role '{role}' required")
        return user

    return role_checker


def require_analyst():
    """Dependency that requires analyst or admin role."""
    return require_role("analyst")


def require_admin():
    """Dependency that requires admin role."""
    return require_role("admin")


# Type aliases for cleaner dependency injection
CurrentUser = Annotated[User, Depends(get_current_user)]
OptionalUser = Annotated[User | None, Depends(get_current_user_optional)]


# =========================
# Audit Logging
# =========================


async def log_analyst_query(
    request: Request,
    user: User,
    entity_type: str | None = None,
    entity_id: str | None = None,
    query_text: str | None = None,
) -> None:
    """Log an analyst query for audit purposes.

    Args:
        request: FastAPI request
        user: Authenticated user
        entity_type: Type of entity being queried
        entity_id: ID of entity being queried
        query_text: Search query text
    """
    from ..db import get_db_session
    from sqlalchemy.dialects.postgresql import insert

    async with get_db_session() as session:
        await session.execute(
            insert("audit_log").values(
                action="query",
                user_id=user.id,
                entity_type=entity_type,
                entity_id=entity_id,
                query_text=query_text,
                request_metadata={
                    "path": str(request.url.path),
                    "method": request.method,
                    "client_ip": request.client.host if request.client else None,
                },
            )
        )
