"""Custom object extraction — dynamically discovered objects."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange


class CustomObjectExtractor(BaseExtractor):
    """Extract data from dynamically-discovered custom objects."""

    def __init__(self, client, custom_object_type: str) -> None:
        super().__init__(client)
        self.object_type = custom_object_type

    def get_all(self, properties: list[str] | None = None, limit: int = 10000) -> pd.DataFrame:
        """Fetch all records for this custom object type."""
        return self.search(
            filter_groups=[],
            properties=properties,
            limit=limit,
        )

    def get_in_time_range(
        self, time_range: TimeRange, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch custom object records created in a time range."""
        return self.search_in_time_range(
            time_range=time_range,
            date_property="createdate",
            properties=properties,
        )
