"""HubSpot API client wrapper with authentication, rate limiting, and retry logic."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import PublicObjectSearchRequest
from urllib3.util.retry import Retry

try:
    import fcntl  # POSIX only; absent on Windows
    _FCNTL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only on Windows
    _FCNTL_AVAILABLE = False


load_dotenv()

log = logging.getLogger(__name__)

RATE_LIMIT_STATE_DIR = Path.home() / ".hubspot_revops"


@dataclass
class RateLimiter:
    """In-process token-bucket rate limiter for HubSpot API calls.

    Used as a fallback when ``SharedRateLimiter`` can't be initialized
    (Windows without ``fcntl``, read-only home directory, etc.).
    """

    max_requests: int = 100
    window_seconds: float = 10.0
    _timestamps: list[float] = field(default_factory=list)

    def wait_if_needed(self) -> None:
        now = time.monotonic()
        # Remove timestamps outside the window
        self._timestamps = [t for t in self._timestamps if now - t < self.window_seconds]
        if len(self._timestamps) >= self.max_requests:
            sleep_time = self.window_seconds - (now - self._timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self._timestamps.append(time.monotonic())


class SharedRateLimiter:
    """Cross-process token-bucket rate limiter backed by a JSON state file.

    HubSpot's CRM API rate limits are per-portal, but the in-process
    :class:`RateLimiter` has no visibility into sibling Python
    processes hitting the same portal. Launching N parallel CLI
    invocations used to give each process its own bucket and the
    aggregate blew past HubSpot's ceiling, triggering 429 cascades
    that exhausted the retry policy and surfaced as
    ``FALLBACK_TO_MCP`` banners on the tail reports.

    This limiter persists the timestamp list in a shared JSON file
    and wraps every read-modify-write in an ``fcntl.flock`` exclusive
    lock. Two parallel processes pointing at the same state path
    automatically serialize through the bucket — the second process
    sees the first's timestamps, finds the bucket full, computes its
    sleep, releases the lock, and retries after sleeping.

    Unix-only via ``fcntl``. On platforms without it the
    :func:`_make_rate_limiter` factory falls back to the in-process
    :class:`RateLimiter`.

    Note: the ``max_requests`` / ``window_seconds`` config is per
    instance, not stored in the file. Two instances pointing at the
    same state path must share the same config or behavior is
    undefined. In practice ``_make_rate_limiter`` enforces this by
    keying state-file names on bucket identity (general vs search).
    """

    def __init__(
        self, max_requests: int, window_seconds: float, state_path: Path
    ) -> None:
        if not _FCNTL_AVAILABLE:
            raise RuntimeError(
                "SharedRateLimiter requires fcntl (POSIX). Use RateLimiter "
                "on Windows or any platform without fcntl."
            )
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.state_path = state_path
        # Ensure the state file exists with readable content so the
        # first locked read doesn't have to handle FileNotFoundError
        # atomically. Permissions 0600 to match the tokens cache.
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            self.state_path.write_text("[]")
            try:
                os.chmod(self.state_path, 0o600)
            except OSError:  # pragma: no cover - best-effort on exotic fs
                pass

    def wait_if_needed(self) -> None:
        """Acquire a slot in the shared bucket, sleeping if necessary.

        Loop pattern: lock → read → prune → claim-or-compute-sleep →
        unlock → sleep (if needed) → retry. The sleep happens *outside*
        the critical section so other waiters can observe our decision
        and queue behind us without deadlocking.
        """
        # Cap the retry loop generously — we should almost never need
        # more than a couple iterations, but a pathological storm of
        # waiters with equal sleep times could in theory loop. Cap
        # protects against any runaway in the logic.
        max_iterations = 200
        for _ in range(max_iterations):
            sleep_time = 0.0
            with open(self.state_path, "r+") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    raw = fh.read().strip()
                    try:
                        timestamps = json.loads(raw) if raw else []
                    except json.JSONDecodeError:
                        log.warning(
                            "rate limit state file %s was corrupted; resetting",
                            self.state_path,
                        )
                        timestamps = []
                    if not isinstance(timestamps, list):
                        timestamps = []
                    now = time.time()  # wall-clock, portable across processes
                    timestamps = [
                        t for t in timestamps
                        if isinstance(t, (int, float))
                        and now - t < self.window_seconds
                    ]
                    if len(timestamps) < self.max_requests:
                        # Slot available — claim it and return.
                        timestamps.append(now)
                        fh.seek(0)
                        fh.truncate()
                        json.dump(timestamps, fh)
                        return
                    # Bucket full — compute how long until the oldest
                    # slot expires. Small jitter (10 ms) prevents a
                    # thundering herd when multiple waiters wake up
                    # at the same instant.
                    sleep_time = (
                        self.window_seconds - (now - timestamps[0]) + 0.01
                    )
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            if sleep_time > 0:
                time.sleep(sleep_time)
        # Pathological: the loop should always find a slot well before
        # this cap. If we do hit it, log and proceed anyway so callers
        # don't hang forever.
        log.error(
            "SharedRateLimiter.wait_if_needed exhausted retries for %s",
            self.state_path,
        )


def _make_rate_limiter(
    max_requests: int,
    window_seconds: float,
    bucket_name: str,
) -> RateLimiter | SharedRateLimiter:
    """Factory: prefer cross-process shared limiter, fall back to in-process.

    Called twice at :class:`HubSpotClient` construction — once for the
    general CRM bucket and once for the stricter Search API bucket.
    Different buckets use different state files so their limits don't
    interfere.

    Returns a plain :class:`RateLimiter` (in-process only) when:
      - ``fcntl`` is unavailable (Windows)
      - the state directory can't be created (read-only filesystem,
        permission denied, etc.)
      - any other OSError surfaces during initialization
    """
    if not _FCNTL_AVAILABLE:
        return RateLimiter(
            max_requests=max_requests, window_seconds=window_seconds
        )
    try:
        state_path = RATE_LIMIT_STATE_DIR / f"rate_limit.{bucket_name}.state.json"
        return SharedRateLimiter(
            max_requests=max_requests,
            window_seconds=window_seconds,
            state_path=state_path,
        )
    except (OSError, PermissionError, RuntimeError) as exc:
        log.warning(
            "shared rate limiter init failed for %s bucket (%s); "
            "falling back to in-process limiter",
            bucket_name,
            exc,
        )
        return RateLimiter(
            max_requests=max_requests, window_seconds=window_seconds
        )


class HubSpotClient:
    """Wrapper around the HubSpot Python SDK with rate limiting and convenience methods."""

    def __init__(self, access_token: str | None = None) -> None:
        if access_token:
            self.access_token = access_token
        elif os.environ.get("HUBSPOT_ACCESS_TOKEN"):
            # CI / headless escape hatch — use a static token directly.
            self.access_token = os.environ["HUBSPOT_ACCESS_TOKEN"]
        else:
            # Interactive OAuth 2.0 flow against a user-registered public app.
            from hubspot_revops.auth import OAuthError, OAuthFlow

            try:
                self.access_token = OAuthFlow.from_env().get_access_token()
            except OAuthError as exc:
                raise ValueError(str(exc)) from exc
        # Longer backoff (1s, 2s, 4s, 8s, 16s) and one extra attempt give the
        # contacts search API breathing room on 5xx spikes — the funnel
        # report used to crash on a single 502.
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        self.api = HubSpot(access_token=self.access_token, retry=retry)
        rate_limit = int(os.environ.get("HUBSPOT_RATE_LIMIT", "100"))
        # Cross-process rate limiting via JSON state files under
        # ~/.hubspot_revops. Multiple parallel CLI invocations now
        # coordinate through a shared bucket instead of each
        # process running its own limiter and collectively blowing
        # past HubSpot's per-portal ceiling. Falls back to the
        # in-process limiter on Windows (no fcntl) or when the state
        # dir can't be created.
        self.rate_limiter = _make_rate_limiter(
            max_requests=rate_limit,
            window_seconds=10.0,
            bucket_name="general",
        )
        # CRM Search API has a stricter limit: 5 req/sec (account-level)
        self.search_rate_limiter = _make_rate_limiter(
            max_requests=5,
            window_seconds=1.0,
            bucket_name="search",
        )

    def _rate_limited(self, func, *args, **kwargs):
        """Execute an API call with rate limiting."""
        self.rate_limiter.wait_if_needed()
        return func(*args, **kwargs)

    # --- CRM Object Operations ---

    def get_objects(self, object_type: str, properties: list[str] | None = None, limit: int = 100):
        """Fetch a page of CRM objects."""
        api = self.api.crm.objects.basic_api
        return self._rate_limited(
            api.get_page,
            object_type=object_type,
            properties=properties or [],
            limit=limit,
        )

    def search_objects(
        self,
        object_type: str,
        filter_groups: list[dict],
        properties: list[str] | None = None,
        sorts: list[dict] | None = None,
        limit: int = 200,
        after: str | None = None,
    ):
        """Search CRM objects with filters. Max 200 per page, 10K total per query."""
        request = PublicObjectSearchRequest(
            filter_groups=filter_groups,
            properties=properties or [],
            sorts=sorts or [],
            limit=min(limit, 200),
            after=after or "0",
        )
        module_name = _sdk_module(object_type)
        sdk_namespace = getattr(self.api.crm, module_name, self.api.crm.objects)
        # Search API has its own 5 req/sec rate limit
        self.search_rate_limiter.wait_if_needed()
        # Typed namespaces (contacts, deals, companies, ...) infer the object
        # type from the namespace itself, so ``do_search`` takes only the
        # search request. The generic ``crm.objects`` search endpoint — used
        # for engagements (meetings/calls/emails/notes/tasks) and custom
        # objects, which have no dedicated SDK namespace — requires
        # ``object_type`` as the first positional argument, otherwise it
        # raises ``TypeError: do_search() missing 1 required positional
        # argument: 'object_type'`` and every activity report crashes.
        if module_name == "objects":
            return self._rate_limited(
                sdk_namespace.search_api.do_search,
                object_type,
                public_object_search_request=request,
            )
        return self._rate_limited(
            sdk_namespace.search_api.do_search,
            public_object_search_request=request,
        )

    # --- Properties / Schema ---

    def get_properties(self, object_type: str):
        """Get all properties for an object type."""
        return self._rate_limited(self.api.crm.properties.core_api.get_all, object_type=object_type)

    def get_schemas(self):
        """Get all custom object schemas."""
        return self._rate_limited(self.api.crm.schemas.core_api.get_all)

    # --- Pipelines ---

    def get_pipelines(self, object_type: str):
        """Get all pipelines for an object type."""
        return self._rate_limited(
            self.api.crm.pipelines.pipelines_api.get_all, object_type=object_type
        )

    # --- Owners ---

    def get_owners(self, limit: int = 500, after: str | None = None):
        """Get a page of HubSpot owners (max 500 per page).

        Accepts an ``after`` cursor so callers can walk the paginated
        results; see :func:`hubspot_revops.extractors.owners.get_owners`
        for the loop that reconciles every page into a single dict.
        """
        kwargs: dict = {"limit": limit}
        if after:
            kwargs["after"] = after
        return self._rate_limited(self.api.crm.owners.owners_api.get_page, **kwargs)

    # --- Associations ---

    def get_associations(self, from_type: str, to_type: str, object_ids: list[str]):
        """Batch read associations between object types."""
        from hubspot.crm.associations.v4 import BatchInputPublicFetchAssociationsBatchRequest

        inputs = [{"id": oid} for oid in object_ids]
        request = BatchInputPublicFetchAssociationsBatchRequest(inputs=inputs)
        return self._rate_limited(
            self.api.crm.associations.v4.batch_api.get_page,
            from_object_type=from_type,
            to_object_type=to_type,
            batch_input_public_fetch_associations_batch_request=request,
        )


def _sdk_module(object_type: str) -> str:
    """Map object type string to SDK module name."""
    mapping = {
        "contacts": "contacts",
        "companies": "companies",
        "deals": "deals",
        "tickets": "tickets",
        "line_items": "line_items",
        "products": "products",
        "quotes": "quotes",
    }
    return mapping.get(object_type, "objects")
