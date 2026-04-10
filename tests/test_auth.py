"""Tests for the HubSpot OAuth 2.0 flow."""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from hubspot_revops.auth import (
    OAuthConfig,
    OAuthError,
    OAuthFlow,
    TokenCache,
)


@pytest.fixture
def tmp_cache_path(tmp_path: Path) -> Path:
    return tmp_path / "tokens.json"


@pytest.fixture
def config(tmp_cache_path: Path) -> OAuthConfig:
    return OAuthConfig(
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_port=18976,
        cache_path=tmp_cache_path,
    )


# --- TokenCache ---


def test_token_cache_roundtrip(tmp_cache_path: Path) -> None:
    cache = TokenCache(tmp_cache_path)
    payload = {
        "access_token": "at-1",
        "refresh_token": "rt-1",
        "expires_at": time.time() + 3600,
        "token_type": "bearer",
    }
    cache.save(payload)
    loaded = cache.load()
    assert loaded == payload


def test_token_cache_load_missing_returns_none(tmp_cache_path: Path) -> None:
    assert TokenCache(tmp_cache_path).load() is None


def test_token_cache_load_corrupt_returns_none(tmp_cache_path: Path) -> None:
    tmp_cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_cache_path.write_text("not json {")
    assert TokenCache(tmp_cache_path).load() is None


def test_token_cache_file_permissions(tmp_cache_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("POSIX permissions only")
    cache = TokenCache(tmp_cache_path)
    cache.save({"access_token": "x", "refresh_token": "y", "expires_at": 0})
    mode = stat.S_IMODE(tmp_cache_path.stat().st_mode)
    assert mode == 0o600


def test_token_cache_clear(tmp_cache_path: Path) -> None:
    cache = TokenCache(tmp_cache_path)
    cache.save({"access_token": "x", "refresh_token": "y", "expires_at": 0})
    assert tmp_cache_path.exists()
    cache.clear()
    assert not tmp_cache_path.exists()
    # clearing a missing file is idempotent
    cache.clear()


def test_is_expired_with_skew() -> None:
    # Expired 10 seconds ago
    assert TokenCache.is_expired({"expires_at": time.time() - 10})
    # Expires in 1 hour → not expired
    assert not TokenCache.is_expired({"expires_at": time.time() + 3600})
    # Expires in 60 seconds but skew is 300 → treated as expired
    assert TokenCache.is_expired({"expires_at": time.time() + 60})


def test_is_expired_missing_field_defaults_expired() -> None:
    assert TokenCache.is_expired({})


# --- OAuthConfig.from_env ---


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HUBSPOT_CLIENT_ID", "cid")
    monkeypatch.setenv("HUBSPOT_CLIENT_SECRET", "secret")
    monkeypatch.delenv("HUBSPOT_REDIRECT_PORT", raising=False)
    cfg = OAuthConfig.from_env()
    assert cfg.client_id == "cid"
    assert cfg.client_secret == "secret"
    assert cfg.redirect_port == 8976
    assert cfg.redirect_uri == "http://localhost:8976/callback"


def test_config_from_env_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HUBSPOT_CLIENT_ID", raising=False)
    monkeypatch.delenv("HUBSPOT_CLIENT_SECRET", raising=False)
    with pytest.raises(OAuthError, match="HUBSPOT_CLIENT_ID"):
        OAuthConfig.from_env()


# --- OAuthFlow token exchange & refresh ---


def _mock_response(status: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json.return_value = payload or {}
    resp.text = json.dumps(payload or {})
    return resp


def test_exchange_code_success(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    payload = {
        "access_token": "new-at",
        "refresh_token": "new-rt",
        "expires_in": 21600,
        "token_type": "bearer",
    }
    with patch("hubspot_revops.auth.httpx.post", return_value=_mock_response(200, payload)) as mock_post:
        result = flow._exchange_code("auth-code-xyz")
    assert result["access_token"] == "new-at"
    assert result["refresh_token"] == "new-rt"
    assert result["expires_at"] > time.time()
    # Verify correct POST data
    _, kwargs = mock_post.call_args
    data = kwargs["data"]
    assert data["grant_type"] == "authorization_code"
    assert data["code"] == "auth-code-xyz"
    assert data["client_id"] == "test-client-id"
    assert data["client_secret"] == "test-client-secret"
    assert data["redirect_uri"] == "http://localhost:18976/callback"


def test_refresh_success(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    payload = {
        "access_token": "refreshed-at",
        "refresh_token": "refreshed-rt",
        "expires_in": 21600,
    }
    with patch("hubspot_revops.auth.httpx.post", return_value=_mock_response(200, payload)):
        result = flow._refresh("old-rt")
    assert result["access_token"] == "refreshed-at"
    assert result["refresh_token"] == "refreshed-rt"


def test_refresh_preserves_refresh_token_when_absent(config: OAuthConfig) -> None:
    """If HubSpot omits refresh_token on a refresh response, we keep the old one."""
    flow = OAuthFlow(config)
    payload = {"access_token": "refreshed-at", "expires_in": 21600}
    with patch("hubspot_revops.auth.httpx.post", return_value=_mock_response(200, payload)):
        result = flow._refresh("old-rt")
    assert result["refresh_token"] == "old-rt"


def test_refresh_invalid_grant_raises(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    with patch(
        "hubspot_revops.auth.httpx.post",
        return_value=_mock_response(400, {"status": "BAD_REFRESH_TOKEN"}),
    ):
        with pytest.raises(OAuthError, match="token exchange failed"):
            flow._refresh("bad-rt")


def test_token_endpoint_network_error_raises(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    with patch(
        "hubspot_revops.auth.httpx.post",
        side_effect=httpx.ConnectError("network down"),
    ):
        with pytest.raises(OAuthError, match="unreachable"):
            flow._exchange_code("code")


# --- OAuthFlow.get_access_token end-to-end paths ---


def test_get_access_token_uses_valid_cache(config: OAuthConfig, tmp_cache_path: Path) -> None:
    cache = TokenCache(tmp_cache_path)
    cache.save(
        {
            "access_token": "cached-at",
            "refresh_token": "cached-rt",
            "expires_at": time.time() + 3600,
        }
    )
    flow = OAuthFlow(config, cache)
    with patch("hubspot_revops.auth.httpx.post") as mock_post:
        token = flow.get_access_token()
    assert token == "cached-at"
    mock_post.assert_not_called()


def test_get_access_token_refreshes_when_expired(config: OAuthConfig, tmp_cache_path: Path) -> None:
    cache = TokenCache(tmp_cache_path)
    cache.save(
        {
            "access_token": "stale-at",
            "refresh_token": "good-rt",
            "expires_at": time.time() - 10,
        }
    )
    flow = OAuthFlow(config, cache)
    refresh_payload = {
        "access_token": "fresh-at",
        "refresh_token": "new-rt",
        "expires_in": 21600,
    }
    with patch(
        "hubspot_revops.auth.httpx.post",
        return_value=_mock_response(200, refresh_payload),
    ) as mock_post:
        token = flow.get_access_token()
    assert token == "fresh-at"
    mock_post.assert_called_once()
    # Cache should be updated on disk
    loaded = cache.load()
    assert loaded["access_token"] == "fresh-at"
    assert loaded["refresh_token"] == "new-rt"


def test_get_access_token_falls_through_on_refresh_failure(
    config: OAuthConfig, tmp_cache_path: Path
) -> None:
    cache = TokenCache(tmp_cache_path)
    cache.save(
        {
            "access_token": "stale-at",
            "refresh_token": "revoked-rt",
            "expires_at": time.time() - 10,
        }
    )
    flow = OAuthFlow(config, cache)

    # Refresh fails → cache cleared → interactive flow invoked. Stub the
    # interactive flow so the test does not touch a browser or the network.
    with (
        patch(
            "hubspot_revops.auth.httpx.post",
            return_value=_mock_response(400, {"status": "BAD_REFRESH_TOKEN"}),
        ),
        patch.object(
            OAuthFlow,
            "_run_authorize_flow",
            return_value={
                "access_token": "interactive-at",
                "refresh_token": "interactive-rt",
                "expires_at": time.time() + 3600,
            },
        ) as mock_interactive,
    ):
        token = flow.get_access_token()

    assert token == "interactive-at"
    mock_interactive.assert_called_once()


def test_get_access_token_runs_interactive_when_no_cache(
    config: OAuthConfig, tmp_cache_path: Path
) -> None:
    flow = OAuthFlow(config)
    with patch.object(
        OAuthFlow,
        "_run_authorize_flow",
        return_value={
            "access_token": "first-at",
            "refresh_token": "first-rt",
            "expires_at": time.time() + 3600,
        },
    ) as mock_interactive:
        token = flow.get_access_token()
    assert token == "first-at"
    mock_interactive.assert_called_once()
    # Persisted
    assert tmp_cache_path.exists()


# --- CSRF / state validation ---


class _FakeServer:
    """Stand-in for the stdlib HTTPServer used inside _run_authorize_flow."""

    def handle_request(self) -> None:  # pragma: no cover - trivial
        pass

    def server_close(self) -> None:  # pragma: no cover - trivial
        pass


def _patched_flow(injected_result: dict):
    """Return patches that simulate the callback handler receiving ``injected_result``.

    The flow internally calls ``_CallbackHandler.result = {}`` right before
    dispatching the thread, so we hook into ``threading.Thread.start`` as the
    point to inject the faked query params.
    """
    import hubspot_revops.auth as auth_mod

    def start_side_effect() -> None:
        auth_mod._CallbackHandler.result = dict(injected_result)

    thread_mock = MagicMock()
    thread_mock.start.side_effect = start_side_effect
    thread_mock.join.return_value = None

    return (
        patch("hubspot_revops.auth.HTTPServer", return_value=_FakeServer()),
        patch("hubspot_revops.auth.webbrowser.open"),
        patch("hubspot_revops.auth.threading.Thread", return_value=thread_mock),
    )


def test_run_authorize_flow_rejects_state_mismatch(config: OAuthConfig) -> None:
    """If the callback state does not match, the flow must raise."""
    flow = OAuthFlow(config)
    server_p, browser_p, thread_p = _patched_flow(
        {"code": "abc", "state": "attacker-state"}
    )
    with server_p, browser_p, thread_p:
        with pytest.raises(OAuthError, match="state mismatch"):
            flow._run_authorize_flow()


def test_run_authorize_flow_rejects_missing_code(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    # Pin the state so the injected callback matches.
    with patch("hubspot_revops.auth.secrets.token_urlsafe", return_value="fixed-state"):
        server_p, browser_p, thread_p = _patched_flow({"state": "fixed-state"})
        with server_p, browser_p, thread_p:
            with pytest.raises(OAuthError, match="no code received"):
                flow._run_authorize_flow()


def test_run_authorize_flow_propagates_hubspot_error(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    server_p, browser_p, thread_p = _patched_flow(
        {"error": "access_denied", "error_description": "User declined"}
    )
    with server_p, browser_p, thread_p:
        with pytest.raises(OAuthError, match="access_denied"):
            flow._run_authorize_flow()


def test_run_authorize_flow_success_end_to_end(config: OAuthConfig) -> None:
    flow = OAuthFlow(config)
    exchange_payload = {
        "access_token": "ok-at",
        "refresh_token": "ok-rt",
        "expires_in": 21600,
    }
    with patch("hubspot_revops.auth.secrets.token_urlsafe", return_value="good-state"):
        server_p, browser_p, thread_p = _patched_flow(
            {"code": "live-code", "state": "good-state"}
        )
        with (
            server_p,
            browser_p,
            thread_p,
            patch(
                "hubspot_revops.auth.httpx.post",
                return_value=_mock_response(200, exchange_payload),
            ) as mock_post,
        ):
            result = flow._run_authorize_flow()
    assert result["access_token"] == "ok-at"
    # Confirm we exchanged the exact code HubSpot returned
    _, kwargs = mock_post.call_args
    assert kwargs["data"]["code"] == "live-code"
