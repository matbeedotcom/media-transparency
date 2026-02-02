"""Meta OAuth Token Service.

Provides CRUD operations for Meta/Facebook OAuth tokens stored in the database.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import get_settings
from ..db import get_db_session
from ..logging import get_context_logger

logger = get_context_logger(__name__)


class MetaOAuthToken(BaseModel):
    """Meta OAuth token model."""

    id: int
    access_token: str
    token_type: str = "user"
    expires_at: datetime | None = None
    scopes: list[str] | None = None
    fb_user_id: str | None = None
    fb_user_name: str | None = None
    created_at: datetime
    updated_at: datetime

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) >= self.expires_at

    @property
    def expires_soon(self) -> bool:
        """Check if token expires within 7 days."""
        if self.expires_at is None:
            return False
        threshold = datetime.now(timezone.utc) + timedelta(days=7)
        return self.expires_at <= threshold

    @property
    def days_until_expiry(self) -> int | None:
        """Get days until token expires."""
        if self.expires_at is None:
            return None
        delta = self.expires_at - datetime.now(timezone.utc)
        return max(0, delta.days)


class MetaTokenService:
    """Service for managing Meta OAuth tokens."""

    META_GRAPH_API_BASE = "https://graph.facebook.com/v24.0"

    def __init__(self):
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def get_active_token(self) -> MetaOAuthToken | None:
        """Get the active (non-expired) Meta token from database.

        Returns:
            MetaOAuthToken if a valid token exists, None otherwise.
        """
        async with get_db_session() as db:
            result = await db.execute(
                text("""
                    SELECT id, access_token, token_type, expires_at, scopes,
                           fb_user_id, fb_user_name, created_at, updated_at
                    FROM meta_oauth_tokens
                    WHERE token_type = 'user'
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
            )
            row = result.first()

            if row is None:
                return None

            token = MetaOAuthToken(
                id=row.id,
                access_token=row.access_token,
                token_type=row.token_type,
                expires_at=row.expires_at,
                scopes=row.scopes,
                fb_user_id=row.fb_user_id,
                fb_user_name=row.fb_user_name,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )

            # Check if expired
            if token.is_expired:
                logger.warning(
                    f"Stored Meta token expired at {token.expires_at}"
                )
                return None

            return token

    async def store_token(
        self,
        access_token: str,
        expires_at: datetime | None = None,
        scopes: list[str] | None = None,
        fb_user_id: str | None = None,
        fb_user_name: str | None = None,
        token_type: str = "user",
    ) -> MetaOAuthToken:
        """Store a new Meta OAuth token.

        This replaces any existing token of the same type.

        Args:
            access_token: The access token string
            expires_at: When the token expires
            scopes: List of granted permission scopes
            fb_user_id: Facebook user ID who authorized
            fb_user_name: Facebook user name who authorized
            token_type: Token type (user, system, app)

        Returns:
            The created MetaOAuthToken
        """
        async with get_db_session() as db:
            # Delete existing tokens of this type (single-user system)
            await db.execute(
                text("DELETE FROM meta_oauth_tokens WHERE token_type = :token_type"),
                {"token_type": token_type},
            )

            # Insert new token
            result = await db.execute(
                text("""
                    INSERT INTO meta_oauth_tokens 
                    (access_token, token_type, expires_at, scopes, fb_user_id, fb_user_name)
                    VALUES (:access_token, :token_type, :expires_at, :scopes, :fb_user_id, :fb_user_name)
                    RETURNING id, created_at, updated_at
                """),
                {
                    "access_token": access_token,
                    "token_type": token_type,
                    "expires_at": expires_at,
                    "scopes": scopes,
                    "fb_user_id": fb_user_id,
                    "fb_user_name": fb_user_name,
                },
            )
            row = result.first()

            logger.info(
                f"Stored new Meta OAuth token for user {fb_user_name or 'unknown'}, "
                f"expires: {expires_at}"
            )

            return MetaOAuthToken(
                id=row.id,
                access_token=access_token,
                token_type=token_type,
                expires_at=expires_at,
                scopes=scopes,
                fb_user_id=fb_user_id,
                fb_user_name=fb_user_name,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )

    async def delete_token(self, token_type: str = "user") -> bool:
        """Delete stored Meta token.

        Args:
            token_type: Type of token to delete

        Returns:
            True if a token was deleted, False otherwise
        """
        async with get_db_session() as db:
            result = await db.execute(
                text("DELETE FROM meta_oauth_tokens WHERE token_type = :token_type"),
                {"token_type": token_type},
            )
            deleted = result.rowcount > 0

            if deleted:
                logger.info(f"Deleted Meta OAuth token (type={token_type})")

            return deleted

    async def exchange_code_for_token(
        self,
        code: str,
        redirect_uri: str,
    ) -> dict:
        """Exchange OAuth authorization code for access token.

        Args:
            code: The authorization code from OAuth callback
            redirect_uri: The redirect URI used in the OAuth flow

        Returns:
            Dict with access_token, token_type, and expires_in

        Raises:
            ValueError: If the exchange fails
        """
        settings = get_settings()

        url = f"{self.META_GRAPH_API_BASE}/oauth/access_token"
        params = {
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "redirect_uri": redirect_uri,
            "code": code,
        }

        response = await self.http_client.get(url, params=params)

        if response.status_code != 200:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", str(response.text))
            except Exception:
                error_msg = response.text[:500] if response.text else "No response"
            raise ValueError(f"Failed to exchange code for token: {error_msg}")

        return response.json()

    async def exchange_for_long_lived_token(
        self,
        short_lived_token: str,
    ) -> tuple[str, datetime]:
        """Exchange a short-lived token for a long-lived token (60 days).

        Args:
            short_lived_token: The short-lived access token

        Returns:
            Tuple of (long_lived_token, expiry_datetime)

        Raises:
            ValueError: If the exchange fails
        """
        settings = get_settings()

        url = f"{self.META_GRAPH_API_BASE}/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": settings.meta_app_id,
            "client_secret": settings.meta_app_secret,
            "fb_exchange_token": short_lived_token,
        }

        response = await self.http_client.get(url, params=params)

        if response.status_code != 200:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", str(response.text))
            except Exception:
                error_msg = response.text[:500] if response.text else "No response"
            raise ValueError(f"Failed to exchange for long-lived token: {error_msg}")

        data = response.json()
        access_token = data["access_token"]
        expires_in = data.get("expires_in", 5184000)  # Default 60 days
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        logger.info(f"Exchanged for long-lived token, expires: {expires_at}")

        return access_token, expires_at

    async def get_token_info(self, access_token: str) -> dict:
        """Get information about an access token (debug token).

        Args:
            access_token: The token to inspect

        Returns:
            Dict with token info including user_id, scopes, expires_at, etc.
        """
        settings = get_settings()

        url = f"{self.META_GRAPH_API_BASE}/debug_token"
        params = {
            "input_token": access_token,
            "access_token": f"{settings.meta_app_id}|{settings.meta_app_secret}",
        }

        response = await self.http_client.get(url, params=params)

        if response.status_code != 200:
            raise ValueError(f"Failed to debug token: {response.text}")

        return response.json().get("data", {})

    async def get_user_info(self, access_token: str) -> dict:
        """Get Facebook user info for the token owner.

        Args:
            access_token: The access token

        Returns:
            Dict with user id and name
        """
        url = f"{self.META_GRAPH_API_BASE}/me"
        params = {
            "access_token": access_token,
            "fields": "id,name",
        }

        response = await self.http_client.get(url, params=params)

        if response.status_code != 200:
            raise ValueError(f"Failed to get user info: {response.text}")

        return response.json()


# Singleton instance
_meta_token_service: MetaTokenService | None = None


def get_meta_token_service() -> MetaTokenService:
    """Get the singleton MetaTokenService instance."""
    global _meta_token_service
    if _meta_token_service is None:
        _meta_token_service = MetaTokenService()
    return _meta_token_service


# Convenience function for getting active token
async def get_active_meta_token() -> MetaOAuthToken | None:
    """Get the active Meta OAuth token from database.

    Convenience function that uses the singleton service.

    Returns:
        MetaOAuthToken if a valid token exists, None otherwise.
    """
    service = get_meta_token_service()
    return await service.get_active_token()
