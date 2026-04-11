"""Base extractor with pagination, search, and association support."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from hubspot_revops.client import HubSpotClient


@dataclass
class TimeRange:
    """A time range for filtering queries."""

    start: datetime
    end: datetime

    @property
    def start_ms(self) -> str:
        return str(int(self.start.timestamp() * 1000))

    @property
    def end_ms(self) -> str:
        return str(int(self.end.timestamp() * 1000))


class BaseExtractor:
    """Base class for CRM data extraction."""

    object_type: str = ""  # Override in subclasses

    def __init__(self, client: HubSpotClient) -> None:
        self.client = client

    def search(
        self,
        filter_groups: list[dict],
        properties: list[str] | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Search objects with filters and return as DataFrame."""
        all_results = []
        after = "0"

        while True:
            response = self.client.search_objects(
                object_type=self.object_type,
                filter_groups=filter_groups,
                properties=properties,
                limit=min(limit, 100),
                after=after,
            )
            for result in response.results:
                row = {"id": result.id, **result.properties}
                all_results.append(row)

            # Check for next page
            if response.paging and response.paging.next and response.paging.next.after:
                after = response.paging.next.after
            else:
                break

            if len(all_results) >= limit:
                break

        return pd.DataFrame(all_results) if all_results else pd.DataFrame()

    def search_in_time_range(
        self,
        time_range: TimeRange,
        date_property: str = "createdate",
        additional_filters: list[dict] | None = None,
        properties: list[str] | None = None,
        limit: int = 10000,
    ) -> pd.DataFrame:
        """Search objects within a time range."""
        filters = [
            {"propertyName": date_property, "operator": "GTE", "value": time_range.start_ms},
            {"propertyName": date_property, "operator": "LTE", "value": time_range.end_ms},
        ]
        if additional_filters:
            filters.extend(additional_filters)

        return self.search(
            filter_groups=[{"filters": filters}],
            properties=properties,
            limit=limit,
        )

    def count(self, filter_groups: list[dict]) -> int:
        """Count matching objects without paginating the full result set.

        Issues a single ``limit=1`` search and reads ``response.total``
        from the first page. This sidesteps the HubSpot CRM Search API's
        10,000-result pagination cap — large portals (e.g. 50k new
        contacts in a quarter) previously had their funnel counts
        silently clipped to 10k because ``search()`` walked the
        ``after`` cursor and exited when the cap was hit. Count-only
        queries do not paginate, so the underlying cap does not apply.
        """
        response = self.client.search_objects(
            object_type=self.object_type,
            filter_groups=filter_groups,
            properties=[],  # minimal payload — we only want metadata
            limit=1,
        )
        total = getattr(response, "total", None)
        return int(total) if total is not None else 0

    def get_associated_ids(self, object_ids: list[str], to_type: str) -> dict[str, list[str]]:
        """Get associated object IDs for a list of source objects."""
        if not object_ids:
            return {}

        result = {}
        # Process in batches of 100
        for i in range(0, len(object_ids), 100):
            batch = object_ids[i : i + 100]
            response = self.client.get_associations(self.object_type, to_type, batch)
            for item in response.results:
                from_id = item.from_.id if hasattr(item, "from_") else item._from.id
                to_ids = [assoc.to_object_id for assoc in item.to]
                result[from_id] = to_ids

        return result
