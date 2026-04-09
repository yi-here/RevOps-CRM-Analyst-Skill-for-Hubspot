"""Deal data extraction with pipeline-aware filtering."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange

DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
    "hubspot_owner_id",
    "hs_deal_stage_probability",
    "hs_forecast_category",
    "hs_is_closed",
    "hs_is_closed_won",
    "deal_currency_code",
    "hs_acv",
    "hs_arr",
    "hs_mrr",
    "hs_tcv",
]


class DealExtractor(BaseExtractor):
    object_type = "deals"

    def get_open_deals(self, properties: list[str] | None = None) -> pd.DataFrame:
        """Fetch all open (non-closed) deals."""
        return self.search(
            filter_groups=[{
                "filters": [
                    {"propertyName": "hs_is_closed", "operator": "EQ", "value": "false"},
                ]
            }],
            properties=properties or DEAL_PROPERTIES,
            limit=10000,
        )

    def get_closed_deals(
        self, time_range: TimeRange, won_only: bool = False, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch deals closed in a time range."""
        filters = [
            {"propertyName": "hs_is_closed", "operator": "EQ", "value": "true"},
        ]
        if won_only:
            filters.append(
                {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"}
            )

        return self.search_in_time_range(
            time_range=time_range,
            date_property="closedate",
            additional_filters=filters,
            properties=properties or DEAL_PROPERTIES,
        )

    def get_deals_by_stage(
        self, stage_id: str, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch deals currently in a specific stage."""
        return self.search(
            filter_groups=[{
                "filters": [
                    {"propertyName": "dealstage", "operator": "EQ", "value": stage_id},
                ]
            }],
            properties=properties or DEAL_PROPERTIES,
        )

    def get_deals_by_owner(
        self, owner_id: str, time_range: TimeRange | None = None, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch deals for a specific owner."""
        filters = [
            {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": owner_id},
        ]
        if time_range:
            return self.search_in_time_range(
                time_range=time_range,
                date_property="createdate",
                additional_filters=filters,
                properties=properties or DEAL_PROPERTIES,
            )
        return self.search(
            filter_groups=[{"filters": filters}],
            properties=properties or DEAL_PROPERTIES,
        )
