"""Company data extraction."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange

COMPANY_PROPERTIES = [
    "name",
    "domain",
    "industry",
    "annualrevenue",
    "numberofemployees",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
    "lifecyclestage",
    "hs_lead_status",
    "country",
    "city",
    "state",
]


class CompanyExtractor(BaseExtractor):
    object_type = "companies"

    def get_companies_by_industry(
        self, industry: str, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch companies in a specific industry."""
        return self.search(
            filter_groups=[{
                "filters": [
                    {"propertyName": "industry", "operator": "EQ", "value": industry},
                ]
            }],
            properties=properties or COMPANY_PROPERTIES,
        )

    def get_new_companies(
        self, time_range: TimeRange, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch companies created in a time range."""
        return self.search_in_time_range(
            time_range=time_range,
            date_property="createdate",
            properties=properties or COMPANY_PROPERTIES,
        )
