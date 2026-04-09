"""Funnel and conversion metrics — lead → MQL → SQL → opportunity → customer."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.contacts import ContactExtractor


LIFECYCLE_STAGES = [
    "subscriber",
    "lead",
    "marketingqualifiedlead",
    "salesqualifiedlead",
    "opportunity",
    "customer",
]

STAGE_DATE_PROPERTIES = {
    "lead": "hs_lifecyclestage_lead_date",
    "marketingqualifiedlead": "hs_lifecyclestage_marketingqualifiedlead_date",
    "salesqualifiedlead": "hs_lifecyclestage_salesqualifiedlead_date",
    "opportunity": "hs_lifecyclestage_opportunity_date",
    "customer": "hs_lifecyclestage_customer_date",
}


def funnel_conversion_rates(
    contact_extractor: ContactExtractor, time_range: TimeRange
) -> dict:
    """Calculate conversion rates between lifecycle stages."""
    contacts = contact_extractor.get_new_contacts(time_range)
    if contacts.empty:
        return {"stages": {}, "total_contacts": 0}

    stage_counts = {}
    for stage in LIFECYCLE_STAGES:
        date_prop = STAGE_DATE_PROPERTIES.get(stage)
        if date_prop and date_prop in contacts.columns:
            reached = contacts[contacts[date_prop].notna()]
            stage_counts[stage] = len(reached)
        elif stage == "subscriber":
            stage_counts[stage] = len(contacts)

    # Calculate step-wise conversion rates
    conversions = {}
    stages = list(stage_counts.keys())
    for i in range(len(stages) - 1):
        from_stage = stages[i]
        to_stage = stages[i + 1]
        from_count = stage_counts.get(from_stage, 0)
        to_count = stage_counts.get(to_stage, 0)
        rate = (to_count / from_count * 100) if from_count > 0 else 0
        conversions[f"{from_stage}_to_{to_stage}"] = {
            "from_count": from_count,
            "to_count": to_count,
            "conversion_rate": round(rate, 1),
        }

    return {
        "stages": stage_counts,
        "conversions": conversions,
        "total_contacts": len(contacts),
    }


def lead_source_breakdown(
    contact_extractor: ContactExtractor, time_range: TimeRange
) -> pd.DataFrame:
    """Break down contacts by original traffic source."""
    contacts = contact_extractor.get_new_contacts(time_range)
    if contacts.empty:
        return pd.DataFrame()

    return contacts.groupby("hs_analytics_source").agg(
        contact_count=("id", "count"),
    ).reset_index().sort_values("contact_count", ascending=False)
