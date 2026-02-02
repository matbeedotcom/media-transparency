"""Meta/Facebook OAuth API endpoints for MITDS.

Provides OAuth flow for connecting Facebook accounts to enable
Meta Ad Library API access with User Access Tokens.
"""

import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from ..config import get_settings
from ..logging import get_context_logger
from ..services.meta_token import MetaOAuthToken, get_meta_token_service
from . import ValidationError

router = APIRouter()
logger = get_context_logger(__name__)

# CSRF state tokens (in production, use Redis or database)
_csrf_states: dict[str, datetime] = {}


# =========================
# Response Models
# =========================


class MetaAuthUrlResponse(BaseModel):
    """Response containing the OAuth URL."""

    auth_url: str
    state: str


class MetaAuthStatusResponse(BaseModel):
    """Response with Meta connection status."""

    connected: bool
    fb_user_id: str | None = None
    fb_user_name: str | None = None
    expires_at: str | None = None
    days_until_expiry: int | None = None
    expires_soon: bool = False
    scopes: list[str] | None = None


class MetaDisconnectResponse(BaseModel):
    """Response after disconnecting Meta."""

    success: bool
    message: str


# =========================
# Helper Functions
# =========================


def _generate_csrf_state() -> str:
    """Generate a CSRF state token."""
    state = secrets.token_urlsafe(32)
    _csrf_states[state] = datetime.now(timezone.utc)
    return state


def _validate_csrf_state(state: str) -> bool:
    """Validate and consume a CSRF state token."""
    if state not in _csrf_states:
        return False
    
    created_at = _csrf_states.pop(state)
    # Tokens expire after 10 minutes
    from datetime import timedelta
    if datetime.now(timezone.utc) - created_at > timedelta(minutes=10):
        return False
    
    return True


def _get_oauth_redirect_uri() -> str:
    """Get the OAuth redirect URI for the callback."""
    settings = get_settings()
    # Use configured redirect URI or construct from API settings
    if hasattr(settings, 'meta_oauth_redirect_uri') and settings.meta_oauth_redirect_uri:
        return settings.meta_oauth_redirect_uri
    # Default construction
    return f"http://localhost:{settings.api_port}/api/v1/meta/auth/callback"


def _get_frontend_redirect_url() -> str:
    """Get the frontend URL to redirect to after OAuth."""
    settings = get_settings()
    if hasattr(settings, 'meta_oauth_frontend_redirect') and settings.meta_oauth_frontend_redirect:
        return settings.meta_oauth_frontend_redirect
    # Default to first CORS origin (usually frontend)
    if settings.cors_origins_list:
        return f"{settings.cors_origins_list[0]}/settings"
    return "http://localhost:5173/settings"


# =========================
# Endpoints
# =========================


@router.get("/auth/login", response_model=MetaAuthUrlResponse)
async def get_meta_auth_url() -> MetaAuthUrlResponse:
    """Get the Facebook OAuth URL to initiate authentication.

    Returns a URL that the frontend should redirect the user to.
    The URL includes:
    - client_id: Your Meta App ID
    - redirect_uri: Callback URL (must be registered in Meta App)
    - scope: Requested permissions (ads_read, pages_read_engagement)
    - state: CSRF token for security

    Returns:
        MetaAuthUrlResponse with auth_url and state token
    """
    settings = get_settings()

    if not settings.meta_app_id:
        raise ValidationError(
            "META_APP_ID is not configured. Set it in your .env file."
        )

    # Validate HTTPS requirement for production
    redirect_uri = _get_oauth_redirect_uri()
    if settings.is_production and not redirect_uri.startswith("https://"):
        raise ValidationError(
            "Meta requires HTTPS for OAuth callback URLs in production. "
            "Set META_OAUTH_REDIRECT_URI to an HTTPS URL."
        )
    
    # Warn in non-localhost development
    if not settings.is_production and not redirect_uri.startswith("https://"):
        if "localhost" not in redirect_uri and "127.0.0.1" not in redirect_uri:
            logger.warning(
                f"OAuth redirect URI {redirect_uri} is not HTTPS. "
                "Meta only allows non-HTTPS for localhost during development."
            )

    # Generate CSRF state token
    state = _generate_csrf_state()

    # Build OAuth URL
    redirect_uri = _get_oauth_redirect_uri()
    
    # Request permissions for Ad Library API and page reading
    scopes = [
        "ads_read",              # Required for Ad Library API
        "pages_read_engagement", # For reading pages user manages
    ]

    params = {
        "client_id": settings.meta_app_id,
        "redirect_uri": redirect_uri,
        "scope": ",".join(scopes),
        "state": state,
        "response_type": "code",
    }

    auth_url = f"https://www.facebook.com/v24.0/dialog/oauth?{urlencode(params)}"

    logger.info(f"Generated Meta OAuth URL with state: {state[:8]}...")

    return MetaAuthUrlResponse(auth_url=auth_url, state=state)


@router.get("/auth/callback")
async def handle_meta_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
) -> RedirectResponse:
    """Handle the OAuth callback from Facebook.

    Facebook redirects here after user grants/denies permissions.
    This endpoint:
    1. Validates the CSRF state token
    2. Exchanges the authorization code for an access token
    3. Exchanges short-lived token for long-lived token (60 days)
    4. Stores the token in the database
    5. Redirects to frontend settings page with success/error status

    Query Parameters:
        code: Authorization code from Facebook (on success)
        state: CSRF state token (must match what we sent)
        error: Error code if user denied or error occurred
        error_description: Human-readable error description

    Returns:
        Redirect to frontend settings page
    """
    frontend_url = _get_frontend_redirect_url()

    # Handle error from Facebook
    if error:
        logger.warning(f"Meta OAuth error: {error} - {error_description}")
        return RedirectResponse(
            url=f"{frontend_url}?meta_error={error}&meta_error_description={error_description or 'Unknown error'}"
        )

    # Validate required parameters
    if not code or not state:
        logger.warning("Meta OAuth callback missing code or state")
        return RedirectResponse(
            url=f"{frontend_url}?meta_error=missing_params&meta_error_description=Missing code or state"
        )

    # Validate CSRF state
    if not _validate_csrf_state(state):
        logger.warning(f"Meta OAuth invalid state: {state[:8]}...")
        return RedirectResponse(
            url=f"{frontend_url}?meta_error=invalid_state&meta_error_description=Invalid or expired state token"
        )

    try:
        service = get_meta_token_service()
        redirect_uri = _get_oauth_redirect_uri()

        # Exchange code for short-lived token
        logger.info("Exchanging authorization code for access token...")
        token_data = await service.exchange_code_for_token(code, redirect_uri)
        short_lived_token = token_data["access_token"]

        # Exchange for long-lived token (60 days)
        logger.info("Exchanging for long-lived token...")
        long_lived_token, expires_at = await service.exchange_for_long_lived_token(
            short_lived_token
        )

        # Get user info
        logger.info("Fetching user info...")
        user_info = await service.get_user_info(long_lived_token)

        # Get token info for scopes
        token_info = await service.get_token_info(long_lived_token)
        scopes = token_info.get("scopes", [])

        # Store token in database
        logger.info(f"Storing token for user: {user_info.get('name')}")
        await service.store_token(
            access_token=long_lived_token,
            expires_at=expires_at,
            scopes=scopes,
            fb_user_id=user_info.get("id"),
            fb_user_name=user_info.get("name"),
            token_type="user",
        )

        logger.info(f"Meta OAuth successful for user: {user_info.get('name')}")

        return RedirectResponse(
            url=f"{frontend_url}?meta_success=true&meta_user={user_info.get('name', 'Unknown')}"
        )

    except Exception as e:
        logger.error(f"Meta OAuth token exchange failed: {e}")
        return RedirectResponse(
            url=f"{frontend_url}?meta_error=token_exchange&meta_error_description={str(e)[:100]}"
        )


@router.get("/auth/status", response_model=MetaAuthStatusResponse)
async def get_meta_status() -> MetaAuthStatusResponse:
    """Get the current Meta connection status.

    Returns information about whether a Facebook account is connected,
    including user details and token expiration.

    Returns:
        MetaAuthStatusResponse with connection details
    """
    service = get_meta_token_service()
    token = await service.get_active_token()

    if not token:
        return MetaAuthStatusResponse(connected=False)

    return MetaAuthStatusResponse(
        connected=True,
        fb_user_id=token.fb_user_id,
        fb_user_name=token.fb_user_name,
        expires_at=token.expires_at.isoformat() if token.expires_at else None,
        days_until_expiry=token.days_until_expiry,
        expires_soon=token.expires_soon,
        scopes=token.scopes,
    )


@router.delete("/auth/disconnect", response_model=MetaDisconnectResponse)
async def disconnect_meta() -> MetaDisconnectResponse:
    """Disconnect the Facebook account.

    Removes the stored OAuth token from the database.
    The system will fall back to META_ACCESS_TOKEN env var if set.

    Returns:
        MetaDisconnectResponse with success status
    """
    service = get_meta_token_service()
    deleted = await service.delete_token()

    if deleted:
        logger.info("Meta OAuth token disconnected")
        return MetaDisconnectResponse(
            success=True,
            message="Facebook account disconnected successfully",
        )
    else:
        return MetaDisconnectResponse(
            success=True,
            message="No Facebook account was connected",
        )


@router.post("/auth/refresh")
async def refresh_meta_token() -> dict[str, Any]:
    """Attempt to refresh the Meta token if expiring soon.

    Note: Meta doesn't support traditional refresh tokens. This endpoint
    checks if the token is expiring soon and prompts re-authentication.

    Returns:
        Status of the refresh attempt
    """
    service = get_meta_token_service()
    token = await service.get_active_token()

    if not token:
        return {
            "success": False,
            "needs_reauth": True,
            "message": "No Meta token stored. Please connect your Facebook account.",
        }

    if token.expires_soon:
        return {
            "success": False,
            "needs_reauth": True,
            "message": f"Token expires in {token.days_until_expiry} days. Please reconnect your Facebook account.",
            "days_until_expiry": token.days_until_expiry,
        }

    return {
        "success": True,
        "needs_reauth": False,
        "message": "Token is still valid",
        "days_until_expiry": token.days_until_expiry,
    }
