"""Funnel and conversion metrics — lead → MQL → SQL → opportunity → customer."""

from __future__ import annotations

import logging

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.contacts import ContactExtractor

log = logging.getLogger(__name__)


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


def _empty_funnel(error: str | None = None) -> dict:
    """Shape for a funnel payload when contacts search fails or is empty."""
    payload: dict = {"stages": {}, "conversions": {}, "total_contacts": 0}
    if error:
        payload["error"] = error
    return payload


def funnel_conversion_rates(
    contact_extractor: ContactExtractor, time_range: TimeRange
) -> dict:
    """Calculate conversion rates between lifecycle stages.

    Returns a ``{"error": ...}`` payload rather than raising when the
    contacts search API is unreachable — the HubSpot contacts endpoint
    occasionally returns 502s under load and the client's retry policy
    only covers transient blips, not sustained outages. The funnel report
    renders a banner instead of crashing the whole command.
    """
    try:
        contacts = contact_extractor.get_new_contacts(time_range)
    except Exception as exc:
        log.warning("contacts search failed in funnel_conversion_rates: %s", exc)
        return _empty_funnel(error=str(exc))
    if contacts.empty:
        return _empty_funnel()

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
    """Break down contacts by original traffic source.

    Returns an empty DataFrame (rather than raising) if contacts search
    fails, so a flaky contacts endpoint does not crash the funnel report.
    """
    try:
        contacts = contact_extractor.get_new_contacts(time_range)
    except Exception as exc:
        log.warning("contacts search failed in lead_source_breakdown: %s", exc)
        return pd.DataFrame()
    if contacts.empty:
        return pd.DataFrame()

    return contacts.groupby("hs_analytics_source").agg(
        contact_count=("id", "count"),
    ).reset_index().sort_values("contact_count", ascending=False)
