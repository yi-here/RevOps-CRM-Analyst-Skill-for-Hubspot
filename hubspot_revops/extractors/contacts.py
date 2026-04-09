"""Contact data extraction with lifecycle stage filtering."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange

CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "lifecyclestage",
    "hs_lead_status",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_lifecyclestage_lead_date",
    "hs_lifecyclestage_marketingqualifiedlead_date",
    "hs_lifecyclestage_salesqualifiedlead_date",
    "hs_lifecyclestage_opportunity_date",
    "hs_lifecyclestage_customer_date",
]


class ContactExtractor(BaseExtractor):
    object_type = "contacts"

    def get_contacts_by_lifecycle(
        self, stage: str, time_range: TimeRange | None = None, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch contacts at a specific lifecycle stage."""
        filters = [
            {"propertyName": "lifecyclestage", "operator": "EQ", "value": stage},
        ]
        if time_range:
            return self.search_in_time_range(
                time_range=time_range,
                date_property="createdate",
                additional_filters=filters,
                properties=properties or CONTACT_PROPERTIES,
            )
        return self.search(
            filter_groups=[{"filters": filters}],
            properties=properties or CONTACT_PROPERTIES,
        )

    def get_contacts_by_source(
        self, source: str, time_range: TimeRange | None = None, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch contacts from a specific analytics source."""
        filters = [
            {"propertyName": "hs_analytics_source", "operator": "EQ", "value": source},
        ]
        if time_range:
            return self.search_in_time_range(
                time_range=time_range,
                date_property="createdate",
                additional_filters=filters,
                properties=properties or CONTACT_PROPERTIES,
            )
        return self.search(
            filter_groups=[{"filters": filters}],
            properties=properties or CONTACT_PROPERTIES,
        )

    def get_new_contacts(
        self, time_range: TimeRange, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch contacts created in a time range."""
        return self.search_in_time_range(
            time_range=time_range,
            date_property="createdate",
            properties=properties or CONTACT_PROPERTIES,
        )
