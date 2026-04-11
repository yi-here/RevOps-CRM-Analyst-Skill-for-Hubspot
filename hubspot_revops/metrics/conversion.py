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

    Uses count-only searches (``limit=1``, reads ``response.total``)
    rather than fetching every matching contact. The HubSpot CRM Search
    API hard-caps result pagination at 10,000 records — on a portal
    with more than 10k new contacts in a period the old implementation
    silently clipped every downstream stage count to whatever it could
    fit in the first 10k, producing wildly under-stated conversion
    rates. ``BaseExtractor.count()`` reads the response metadata and
    never walks the ``after`` cursor, so the cap does not apply.

    Returns a ``{"error": ...}`` payload rather than raising when the
    contacts search API is unreachable — the HubSpot contacts endpoint
    occasionally returns 502s under load and the client's retry policy
    only covers transient blips, not sustained outages. The funnel
    report renders a banner instead of crashing the whole command.
    """
    time_filters = [
        {"propertyName": "createdate", "operator": "GTE", "value": time_range.start_ms},
        {"propertyName": "createdate", "operator": "LTE", "value": time_range.end_ms},
    ]

    def _count(extra_filters: list[dict]) -> int:
        return contact_extractor.count(
            [{"filters": time_filters + extra_filters}]
        )

    try:
        total_contacts = _count([])
    except Exception as exc:
        log.warning("contacts count failed in funnel_conversion_rates: %s", exc)
        return _empty_funnel(error=str(exc))

    if total_contacts == 0:
        return _empty_funnel()

    # Subscriber count == total contacts created in the range (anyone
    # who entered HubSpot is at minimum a subscriber). Downstream stages
    # are counted via HAS_PROPERTY on the stage-entry date, preserving
    # the prior semantics: "contacts created in period who ever reached
    # stage X".
    stage_counts: dict[str, int] = {"subscriber": total_contacts}
    for stage in LIFECYCLE_STAGES:
        if stage == "subscriber":
            continue
        date_prop = STAGE_DATE_PROPERTIES.get(stage)
        if not date_prop:
            continue
        try:
            stage_counts[stage] = _count(
                [{"propertyName": date_prop, "operator": "HAS_PROPERTY"}]
            )
        except Exception as exc:
            log.warning(
                "contacts count failed for stage %s: %s", stage, exc
            )
            return _empty_funnel(error=str(exc))

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
        "total_contacts": total_contacts,
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
