"""Tests for the cross-process SharedRateLimiter."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from hubspot_revops.client import (
    RateLimiter,
    SharedRateLimiter,
    _make_rate_limiter,
)


def test_shared_rate_limiter_persists_state_to_disk(tmp_path: Path):
    """First call must write a single timestamp to the state file so
    a second process starting cold can see the slot is in use."""
    state_path = tmp_path / "rate_limit.test.state.json"
    limiter = SharedRateLimiter(
        max_requests=5,
        window_seconds=10.0,
        state_path=state_path,
    )

    limiter.wait_if_needed()

    data = json.loads(state_path.read_text())
    assert isinstance(data, list)
    assert len(data) == 1
    assert isinstance(data[0], (int, float))
    # The timestamp must be recent (within the last second).
    assert abs(time.time() - data[0]) < 1.0


def test_shared_rate_limiter_coordinates_across_instances(tmp_path: Path):
    """Two SharedRateLimiter instances pointing at the same state file
    must share the bucket — simulates two parallel Python processes
    hitting the same portal. Both calls should land in the same list."""
    state_path = tmp_path / "rate_limit.test.state.json"

    a = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )
    b = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )

    a.wait_if_needed()
    a.wait_if_needed()
    b.wait_if_needed()

    data = json.loads(state_path.read_text())
    assert len(data) == 3


def test_shared_rate_limiter_reads_existing_state_on_startup(tmp_path: Path):
    """A fresh limiter instance must observe timestamps written by a
    previous process instead of starting with an empty bucket."""
    state_path = tmp_path / "rate_limit.test.state.json"
    now = time.time()
    state_path.write_text(
        json.dumps([now - 1, now - 0.5, now - 0.1, now - 0.05])
    )

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )
    limiter.wait_if_needed()

    data = json.loads(state_path.read_text())
    assert len(data) == 5


def test_shared_rate_limiter_prunes_expired_timestamps(tmp_path: Path):
    """Timestamps older than the window must be dropped so a bucket
    that was saturated ages ago doesn't block new callers forever."""
    state_path = tmp_path / "rate_limit.test.state.json"
    old = time.time() - 100  # 100s ago, far outside a 10s window
    state_path.write_text(json.dumps([old, old, old]))

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )
    limiter.wait_if_needed()

    data = json.loads(state_path.read_text())
    # The three ancient timestamps are pruned; one new one added.
    assert len(data) == 1
    assert abs(time.time() - data[0]) < 1.0


def test_shared_rate_limiter_handles_corrupted_json(tmp_path: Path):
    """A partially-written state file from a crashed process must be
    recovered, not crashed over. Garbage → empty bucket."""
    state_path = tmp_path / "rate_limit.test.state.json"
    state_path.write_text("not json {{{")

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )
    limiter.wait_if_needed()

    data = json.loads(state_path.read_text())
    assert isinstance(data, list)
    assert len(data) == 1


def test_shared_rate_limiter_handles_non_list_state(tmp_path: Path):
    """A file whose JSON parses but isn't a list (e.g. someone wrote
    a dict there) must be recovered the same way."""
    state_path = tmp_path / "rate_limit.test.state.json"
    state_path.write_text(json.dumps({"not": "a list"}))

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=state_path
    )
    limiter.wait_if_needed()

    data = json.loads(state_path.read_text())
    assert isinstance(data, list)
    assert len(data) == 1


def test_shared_rate_limiter_sleeps_when_bucket_full(tmp_path: Path):
    """A saturated bucket must force the caller to sleep until the
    oldest slot expires. We verify by pre-filling with max_requests
    fresh timestamps and observing that wait_if_needed takes at
    least a measurable amount of time before returning."""
    state_path = tmp_path / "rate_limit.test.state.json"
    now = time.time()
    # Five fresh timestamps with a 0.3s window → bucket is full, the
    # oldest slot expires in ~0.3 seconds.
    state_path.write_text(json.dumps([now, now, now, now, now]))

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=0.3, state_path=state_path
    )

    start = time.monotonic()
    limiter.wait_if_needed()
    elapsed = time.monotonic() - start

    # Should have slept at least close to the full window. Allow
    # generous slack for slow CI — the key assertion is "did NOT
    # return immediately", which would indicate the bucket was
    # ignored.
    assert elapsed >= 0.2, (
        f"expected at least ~0.3s of sleep, got {elapsed:.3f}s — "
        f"bucket saturation was not respected"
    )


def test_shared_rate_limiter_creates_state_dir_if_missing(tmp_path: Path):
    """A fresh install should not fail because ~/.hubspot_revops
    doesn't exist yet."""
    nested_path = tmp_path / "nonexistent" / "deep" / "dir" / "state.json"
    assert not nested_path.parent.exists()

    limiter = SharedRateLimiter(
        max_requests=5, window_seconds=10.0, state_path=nested_path
    )
    limiter.wait_if_needed()

    assert nested_path.exists()
    assert nested_path.parent.is_dir()


def test_make_rate_limiter_returns_shared_limiter_when_available(tmp_path: Path):
    """The factory must return a SharedRateLimiter when fcntl is
    available and the state dir is writable."""
    with patch("hubspot_revops.client.RATE_LIMIT_STATE_DIR", tmp_path):
        limiter = _make_rate_limiter(
            max_requests=10, window_seconds=5.0, bucket_name="general"
        )

    assert isinstance(limiter, SharedRateLimiter)
    assert limiter.max_requests == 10
    assert limiter.window_seconds == 5.0
    assert limiter.state_path.name == "rate_limit.general.state.json"


def test_make_rate_limiter_falls_back_when_state_path_unwritable(tmp_path: Path):
    """If SharedRateLimiter construction raises OSError (read-only
    filesystem, permission denied, etc.), the factory must degrade
    gracefully to the in-process RateLimiter rather than crashing
    HubSpotClient construction."""
    with patch(
        "hubspot_revops.client.SharedRateLimiter",
        side_effect=OSError("read-only filesystem"),
    ):
        limiter = _make_rate_limiter(
            max_requests=10, window_seconds=5.0, bucket_name="general"
        )

    assert isinstance(limiter, RateLimiter)
    assert not isinstance(limiter, SharedRateLimiter)
    assert limiter.max_requests == 10


def test_make_rate_limiter_falls_back_on_windows(tmp_path: Path):
    """On Windows (no fcntl) the factory must return the in-process
    RateLimiter without attempting to construct SharedRateLimiter."""
    with patch("hubspot_revops.client._FCNTL_AVAILABLE", False):
        limiter = _make_rate_limiter(
            max_requests=10, window_seconds=5.0, bucket_name="general"
        )

    assert isinstance(limiter, RateLimiter)
    assert not isinstance(limiter, SharedRateLimiter)


def test_make_rate_limiter_general_and_search_use_different_state_files(tmp_path: Path):
    """Bucket identity must key on the state filename so general and
    search API limits don't interfere with each other."""
    with patch("hubspot_revops.client.RATE_LIMIT_STATE_DIR", tmp_path):
        general = _make_rate_limiter(
            max_requests=100, window_seconds=10.0, bucket_name="general"
        )
        search = _make_rate_limiter(
            max_requests=5, window_seconds=1.0, bucket_name="search"
        )

    assert isinstance(general, SharedRateLimiter)
    assert isinstance(search, SharedRateLimiter)
    assert general.state_path != search.state_path
    assert "general" in general.state_path.name
    assert "search" in search.state_path.name


def test_shared_rate_limiter_does_not_interfere_with_other_bucket(tmp_path: Path):
    """Two limiters at different state paths must have fully
    independent buckets — claims on A don't consume B's slots."""
    path_a = tmp_path / "rate_limit.general.state.json"
    path_b = tmp_path / "rate_limit.search.state.json"

    a = SharedRateLimiter(max_requests=2, window_seconds=10.0, state_path=path_a)
    b = SharedRateLimiter(max_requests=2, window_seconds=10.0, state_path=path_b)

    a.wait_if_needed()
    a.wait_if_needed()
    b.wait_if_needed()

    assert len(json.loads(path_a.read_text())) == 2
    assert len(json.loads(path_b.read_text())) == 1
