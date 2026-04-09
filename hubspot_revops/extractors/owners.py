"""Owner (user) extraction."""

from __future__ import annotations

from hubspot_revops.client import HubSpotClient
from hubspot_revops.schema.models import Owner


def get_owners(client: HubSpotClient) -> dict[str, Owner]:
    """Fetch all HubSpot owners and return as a dict keyed by owner ID."""
    response = client.get_owners(limit=100)
    owners = {}
    for o in response.results:
        owners[o.id] = Owner(
            owner_id=o.id,
            email=getattr(o, "email", ""),
            first_name=getattr(o, "first_name", ""),
            last_name=getattr(o, "last_name", ""),
        )
    return owners
