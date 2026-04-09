"""HubSpot API client wrapper with authentication, rate limiting, and retry logic."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import PublicObjectSearchRequest
from urllib3.util.retry import Retry


load_dotenv()


@dataclass
class RateLimiter:
    """Token-bucket rate limiter for HubSpot API calls."""

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


class HubSpotClient:
    """Wrapper around the HubSpot Python SDK with rate limiting and convenience methods."""

    def __init__(self, access_token: str | None = None) -> None:
        self.access_token = access_token or os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
        if not self.access_token:
            raise ValueError(
                "HUBSPOT_ACCESS_TOKEN is required. "
                "Set it as an environment variable or pass it to HubSpotClient()."
            )
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 504))
        self.api = HubSpot(access_token=self.access_token, retry=retry)
        rate_limit = int(os.environ.get("HUBSPOT_RATE_LIMIT", "100"))
        self.rate_limiter = RateLimiter(max_requests=rate_limit)
        # CRM Search API has a stricter limit: 5 req/sec (account-level)
        self.search_rate_limiter = RateLimiter(max_requests=5, window_seconds=1.0)

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
        search_api = getattr(self.api.crm, _sdk_module(object_type), self.api.crm.objects)
        # Search API has its own 5 req/sec rate limit
        self.search_rate_limiter.wait_if_needed()
        return self._rate_limited(search_api.search_api.do_search, public_object_search_request=request)

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

    def get_owners(self, limit: int = 500):
        """Get all HubSpot owners (max 500 per page)."""
        return self._rate_limited(self.api.crm.owners.owners_api.get_page, limit=limit)

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
