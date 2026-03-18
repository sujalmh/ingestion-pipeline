"""
Auth Manager — Token/API key management with auto-refresh.

Handles authentication for data sources:
- token: Login with credentials, get expiring bearer token (MoSPI pattern)
- api_key: Static key from env vars or direct values
- none: No auth required

Pattern: Module-level async functions + dict-based token cache (adapter-style)
"""
import logging
import os
import time
from typing import Any, Dict, Optional

import httpx

from app.agent.source_config import SourceConfig
from app.config import settings

logger = logging.getLogger(__name__)


class AuthError(Exception):
    """Raised when authentication fails and the request should not proceed."""
    pass


# ============ Module-Level Token Cache ============

# {source_id: {"token": str, "expires_at": float}}
_token_cache: Dict[str, Dict] = {}


# ============ Public Functions ============

async def get_auth_headers(source_config: SourceConfig) -> Dict[str, str]:
    """
    Get authentication headers for a data source.
    Automatically refreshes expired tokens.

    Args:
        source_config: The source configuration.

    Returns:
        Dict of HTTP headers to include in API requests.

    Raises:
        AuthError: If authentication is required but fails.
    """
    auth_type = source_config.auth.type

    if auth_type == "none":
        return {}

    elif auth_type == "token":
        token = await _get_valid_token(source_config)
        if token:
            return {"Authorization": f"Bearer {token}"}
        raise AuthError(
            f"Failed to obtain auth token for source: {source_config.source_id}. "
            "Check credentials and login URL."
        )

    elif auth_type == "api_key":
        api_key = _resolve_api_key(source_config)
        if not api_key:
            raise AuthError(
                f"Failed to resolve API key for source: {source_config.source_id}. "
                "Check credentials_env configuration."
            )
        placement = _get_api_key_placement(source_config)
        header_name = placement.get("header", "Authorization")
        prefix = placement.get("prefix", "Bearer ")
        return {header_name: f"{prefix}{api_key}"}

    else:
        logger.warning(f"Unknown auth type '{auth_type}' for source: {source_config.source_id}")
        return {}


# ============ Token Management ============

async def _get_valid_token(source_config: SourceConfig) -> Optional[str]:
    """
    Get a valid token for the source, refreshing if expired.

    Args:
        source_config: The source configuration.

    Returns:
        Valid token string, or None if login fails.
    """
    sid = source_config.source_id

    if sid in _token_cache and not _is_token_expired(sid):
        return _token_cache[sid]["token"]

    # Need to login for a fresh token
    return await _login_for_token(source_config)


async def _login_for_token(source_config: SourceConfig) -> Optional[str]:
    """
    Call login endpoint to obtain a bearer token.
    Caches the token with expiry timestamp.

    Sends all resolved credentials as the JSON body, so it works
    regardless of whether the API expects email/password, username/password,
    client_id/client_secret, etc.

    Args:
        source_config: The source configuration with auth details.

    Returns:
        Token string, or None on failure.
    """
    sid = source_config.source_id
    creds = _resolve_credentials(source_config)

    if not creds:
        logger.error(f"No credentials resolved for source: {sid}")
        return None

    login_url = source_config.base_url.rstrip("/") + source_config.auth.login_url

    try:
        timeout = httpx.Timeout(float(settings.AGENT_FETCH_TIMEOUT))
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                login_url,
                json=creds,
                headers={"Content-Type": "application/json"},
            )

        if resp.status_code in (200, 201):
            data = resp.json()
            token = _extract_token(data)
            if token:
                expiry_minutes = source_config.auth.token_expiry_minutes
                _token_cache[sid] = {
                    "token": token,
                    "expires_at": time.time() + (expiry_minutes * 60),
                }
                logger.info(f"Obtained auth token for source: {sid} (expires in {expiry_minutes}m)")
                return token

            logger.error(f"No token found in login response for source: {sid}. Keys: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}")
            return None

        logger.error(f"Login failed for source: {sid}. Status: {resp.status_code}, Body: {resp.text[:500]}")
        return None

    except Exception as e:
        logger.error(f"Login request failed for source: {sid}: {e}")
        return None


def _extract_token(data: Any) -> Optional[str]:
    """
    Extract a token from various login response shapes.
    Searches common field names recursively (one level deep).
    """
    if not isinstance(data, dict):
        return None
    _TOKEN_KEYS = ("token", "access_token", "accessToken",
                   "id_token", "jwt", "auth_token", "bearer")
    for key in _TOKEN_KEYS:
        val = data.get(key)
        if isinstance(val, str) and val:
            return val
    for val in data.values():
        if isinstance(val, dict):
            for key in _TOKEN_KEYS:
                inner = val.get(key)
                if isinstance(inner, str) and inner:
                    return inner
    return None


def _is_token_expired(source_id: str) -> bool:
    """Check if the cached token for a source is expired."""
    cached = _token_cache.get(source_id)
    if not cached:
        return True
    return time.time() >= cached["expires_at"]


# ============ Credential Resolution ============

def _resolve_credentials(source_config: SourceConfig) -> Dict[str, str]:
    """
    Resolve credentials — checks env vars first, then falls back to direct values.

    YAML configs use env var names:  {"email": "MOSPI_EMAIL", "password": "MOSPI_PASSWORD"}
    DB configs may have direct values:  {"email": "user@example.com", "password": "pass123"}

    Args:
        source_config: The source configuration.

    Returns:
        Dict with resolved credential values.
    """
    creds = source_config.auth.credentials_env or {}
    resolved: Dict[str, str] = {}

    for key, value in creds.items():
        if key.endswith("_env"):
            # Explicit env var reference — resolve from environment
            actual_key = key.replace("_env", "")
            resolved[actual_key] = os.getenv(value, "")
        else:
            # Try as env var name first, then use as direct value
            env_val = os.getenv(value, "")
            if env_val:
                resolved[key] = env_val
            else:
                resolved[key] = value

    return resolved


def _resolve_api_key(source_config: SourceConfig) -> Optional[str]:
    """
    Resolve API key from credentials.

    Args:
        source_config: The source configuration.

    Returns:
        API key string, or None if not found.
    """
    creds = _resolve_credentials(source_config)
    return (creds.get("api_key") or creds.get("key")
            or creds.get("apikey") or creds.get("token"))


def _get_api_key_placement(source_config: SourceConfig) -> Dict[str, str]:
    """
    Determine where to place the API key in the request.

    Checks auth config for 'header' and 'prefix' overrides.
    Defaults to Authorization: Bearer <key>.

    Supports patterns like:
      - X-API-Key: <key>  (header="X-API-Key", prefix="")
      - Authorization: Bearer <key>  (default)
      - Authorization: Token <key>  (prefix="Token ")
    """
    creds = source_config.auth.credentials_env or {}
    return {
        "header": creds.get("header_name", "Authorization"),
        "prefix": creds.get("header_prefix", "Bearer "),
    }


def clear_token_cache(source_id: Optional[str] = None):
    """
    Clear cached tokens. Useful for testing or forced re-auth.

    Args:
        source_id: Clear token for a specific source, or all if None.
    """
    global _token_cache
    if source_id:
        _token_cache.pop(source_id, None)
    else:
        _token_cache.clear()
