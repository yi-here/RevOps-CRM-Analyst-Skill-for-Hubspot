"""HubSpot OAuth 2.0 authentication flow.

Implements a browser-based OAuth 2.0 authorization code flow against a
user-registered HubSpot public app. Tokens are cached on disk and refreshed
silently; the interactive browser flow only runs on first use or when the
refresh token has been revoked.

For CI / headless environments, set ``HUBSPOT_ACCESS_TOKEN`` and the caller
(``HubSpotClient``) will skip this module entirely.
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import httpx


DEFAULT_SCOPES = [
    "crm.objects.contacts.read",
    "crm.objects.companies.read",
    "crm.objects.deals.read",
    "crm.objects.line_items.read",
    "crm.schemas.custom.read",
    "crm.objects.owners.read",
    "sales-email-read",
    "oauth",
]

AUTHORIZE_URL = "https://app.hubspot.com/oauth/authorize"
TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
DEFAULT_REDIRECT_PORT = 8976
DEFAULT_CACHE_PATH = Path.home() / ".hubspot_revops" / "tokens.json"
EXPIRY_SKEW_SECONDS = 300  # refresh 5 minutes before actual expiry


class OAuthError(RuntimeError):
    """Raised when OAuth configuration or token exchange fails."""


@dataclass
class OAuthConfig:
    """OAuth app registration and flow configuration."""

    client_id: str
    client_secret: str
    redirect_port: int = DEFAULT_REDIRECT_PORT
    scopes: list[str] = field(default_factory=lambda: list(DEFAULT_SCOPES))
    cache_path: Path = DEFAULT_CACHE_PATH

    @property
    def redirect_uri(self) -> str:
        return f"http://localhost:{self.redirect_port}/callback"

    @classmethod
    def from_env(cls) -> OAuthConfig:
        """Build an ``OAuthConfig`` from environment variables.

        Reads ``HUBSPOT_CLIENT_ID``, ``HUBSPOT_CLIENT_SECRET``, and the
        optional ``HUBSPOT_REDIRECT_PORT``. Raises ``OAuthError`` if the
        required variables are missing.
        """
        client_id = os.environ.get("HUBSPOT_CLIENT_ID", "").strip()
        client_secret = os.environ.get("HUBSPOT_CLIENT_SECRET", "").strip()
        if not client_id or not client_secret:
            raise OAuthError(
                "HubSpot OAuth is not configured. Set HUBSPOT_CLIENT_ID and "
                "HUBSPOT_CLIENT_SECRET from your HubSpot public app "
                "(https://developers.hubspot.com), or set HUBSPOT_ACCESS_TOKEN "
                "to use a static Private App token instead."
            )
        port = int(os.environ.get("HUBSPOT_REDIRECT_PORT", DEFAULT_REDIRECT_PORT))
        return cls(client_id=client_id, client_secret=client_secret, redirect_port=port)


class TokenCache:
    """On-disk JSON token store with ``0600`` file permissions."""

    def __init__(self, path: Path = DEFAULT_CACHE_PATH) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def save(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass  # Windows / restricted filesystem
        os.replace(tmp, self.path)

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    @staticmethod
    def is_expired(data: dict[str, Any], skew: int = EXPIRY_SKEW_SECONDS) -> bool:
        expires_at = data.get("expires_at", 0)
        return time.time() + skew >= expires_at


class _CallbackHandler(BaseHTTPRequestHandler):
    """One-shot HTTP handler that captures the OAuth callback query params."""

    # Set by OAuthFlow before serving
    result: dict[str, str] = {}

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return
        params = urllib.parse.parse_qs(parsed.query)
        # Record only the first value for each key
        type(self).result.update({k: v[0] for k, v in params.items() if v})
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = (
            "<html><body style='font-family:sans-serif;max-width:32rem;margin:4rem auto;'>"
            "<h2>HubSpot authorization complete</h2>"
            "<p>You can close this window and return to your terminal.</p>"
            "</body></html>"
        )
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # Silence default stderr access log
        return


class OAuthFlow:
    """Manages HubSpot OAuth tokens: cache hit → refresh → interactive flow."""

    def __init__(self, config: OAuthConfig, cache: TokenCache | None = None) -> None:
        self.config = config
        self.cache = cache or TokenCache(config.cache_path)

    @classmethod
    def from_env(cls) -> OAuthFlow:
        config = OAuthConfig.from_env()
        return cls(config)

    # --- Public API ---

    def get_access_token(self) -> str:
        """Return a valid access token, running the OAuth flow as needed."""
        cached = self.cache.load()
        if cached and not TokenCache.is_expired(cached):
            return cached["access_token"]

        if cached and cached.get("refresh_token"):
            try:
                refreshed = self._refresh(cached["refresh_token"])
                self._persist(refreshed)
                return refreshed["access_token"]
            except OAuthError:
                # Refresh token rejected — fall through to interactive flow
                self.cache.clear()

        tokens = self._run_authorize_flow()
        self._persist(tokens)
        return tokens["access_token"]

    # --- Internal steps ---

    def _run_authorize_flow(self) -> dict[str, Any]:
        state = secrets.token_urlsafe(32)
        authorize_url = self._build_authorize_url(state)

        server = HTTPServer(("localhost", self.config.redirect_port), _CallbackHandler)
        _CallbackHandler.result = {}
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()

        print(
            f"Opening browser to authorize HubSpot access...\n"
            f"  If the browser does not open, visit:\n  {authorize_url}\n"
        )
        try:
            webbrowser.open(authorize_url)
        except Exception:
            pass  # User can still paste the URL manually

        thread.join(timeout=300)  # 5 minute cap on the interactive flow
        server.server_close()

        result = dict(_CallbackHandler.result)
        if "error" in result:
            raise OAuthError(
                f"HubSpot returned an error during authorization: "
                f"{result.get('error')} - {result.get('error_description', '')}"
            )
        if result.get("state") != state:
            raise OAuthError(
                "OAuth state mismatch — the callback did not match the original "
                "authorization request. This may indicate a CSRF attempt."
            )
        code = result.get("code")
        if not code:
            raise OAuthError(
                "OAuth authorization did not complete (no code received). "
                "Check that the redirect URI on your HubSpot app matches "
                f"{self.config.redirect_uri}."
            )
        return self._exchange_code(code)

    def _build_authorize_url(self, state: str) -> str:
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "scope": " ".join(self.config.scopes),
            "state": state,
        }
        return f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

    def _exchange_code(self, code: str) -> dict[str, Any]:
        data = {
            "grant_type": "authorization_code",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "redirect_uri": self.config.redirect_uri,
            "code": code,
        }
        return self._post_token(data)

    def _refresh(self, refresh_token: str) -> dict[str, Any]:
        data = {
            "grant_type": "refresh_token",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "refresh_token": refresh_token,
        }
        return self._post_token(data)

    def _post_token(self, data: dict[str, str]) -> dict[str, Any]:
        try:
            response = httpx.post(
                TOKEN_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30.0,
            )
        except httpx.HTTPError as exc:
            raise OAuthError(f"HubSpot token endpoint unreachable: {exc}") from exc
        if response.status_code != 200:
            raise OAuthError(
                f"HubSpot token exchange failed ({response.status_code}): "
                f"{response.text}"
            )
        payload = response.json()
        # Preserve the refresh token across refreshes (HubSpot reissues it,
        # but we keep the existing one as a fallback if a new one is absent).
        if "refresh_token" not in payload and data.get("refresh_token"):
            payload["refresh_token"] = data["refresh_token"]
        payload["expires_at"] = time.time() + int(payload.get("expires_in", 0))
        return payload

    def _persist(self, tokens: dict[str, Any]) -> None:
        record = {
            "access_token": tokens["access_token"],
            "refresh_token": tokens.get("refresh_token", ""),
            "expires_at": tokens.get("expires_at", 0),
            "token_type": tokens.get("token_type", "bearer"),
        }
        self.cache.save(record)
