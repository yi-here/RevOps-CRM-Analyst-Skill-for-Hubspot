"""Owner (user) extraction."""

from __future__ import annotations

from hubspot_revops.client import HubSpotClient
from hubspot_revops.schema.models import Owner


def get_owners(client: HubSpotClient) -> dict[str, Owner]:
    """Fetch all HubSpot owners and return as a dict keyed by owner ID.

    The owners endpoint is paginated — the previous single-page fetch
    silently dropped everyone past the first 100, which is why ~40 rows
    in the team and forecast reports showed raw numeric IDs instead of
    names. Walk every page here until the ``next`` cursor is empty.
    """
    owners: dict[str, Owner] = {}
    after: str | None = None
    # Max 500 per page is the documented HubSpot limit; a few extra
    # iterations are cheap next to the API cost, and capping the loop at
    # 100 pages (~50k owners) guards against a misbehaving cursor.
    for _ in range(100):
        response = client.get_owners(limit=500, after=after)
        for o in response.results:
            owners[o.id] = Owner(
                owner_id=o.id,
                email=getattr(o, "email", "") or "",
                first_name=getattr(o, "first_name", "") or "",
                last_name=getattr(o, "last_name", "") or "",
            )
        paging = getattr(response, "paging", None)
        nxt = getattr(paging, "next", None) if paging else None
        after = getattr(nxt, "after", None) if nxt else None
        if not after:
            break
    return owners
