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
        """Search objects with filters and return as DataFrame.

        The HubSpot Search API caps each query at 10,000 total results
        regardless of pagination. Callers that only need an accurate
        **count** (not the rows themselves) should use
        :meth:`count_via_search` instead, which reads ``response.total``
        from the server and is not capped by the page window.

        The returned DataFrame's ``.attrs["hs_total"]`` carries the
        server-reported total so downstream code can detect truncation
        without a second API call.
        """
        all_results = []
        after = "0"
        hs_total: int | None = None

        while True:
            response = self.client.search_objects(
                object_type=self.object_type,
                filter_groups=filter_groups,
                properties=properties,
                limit=min(limit, 100),
                after=after,
            )
            if hs_total is None:
                hs_total = getattr(response, "total", None)
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

        df = pd.DataFrame(all_results) if all_results else pd.DataFrame()
        if hs_total is not None:
            df.attrs["hs_total"] = int(hs_total)
        return df

    def count_via_search(
        self,
        filter_groups: list[dict],
    ) -> int:
        """Return the full server-reported count for a search query.

        The HubSpot Search API attaches ``total`` to every response,
        which reports the true number of matches even when the caller
        asks for zero rows. Previously the funnel report used
        ``len(get_new_contacts(...))`` to size the top of the funnel,
        which was capped at the 10,000-row pagination window — so a
        portal with 7.1M contacts showed 10K. Reading ``response.total``
        directly bypasses the cap and costs a single API call with
        ``limit=1`` and no properties.
        """
        response = self.client.search_objects(
            object_type=self.object_type,
            filter_groups=filter_groups,
            properties=[],
            limit=1,
            after="0",
        )
        return int(getattr(response, "total", 0) or 0)

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
