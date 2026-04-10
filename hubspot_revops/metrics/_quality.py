"""Pipeline hygiene / data quality helpers.

These are shared between the closed-lost report and executive summary;
they identify deals that should be cleaned up:

- **stale open deals** — close date is in the past but the deal is still
  marked open.
- **zero-engagement deals** — no associated meetings, calls, emails, or
  notes. These are "ghost deals" that never actually got worked.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_bool_series

ENGAGEMENT_TYPES = ("meetings", "calls", "emails", "notes")


def find_stale_open_deals(
    df: pd.DataFrame, now: datetime | None = None
) -> pd.DataFrame:
    """Return deals whose close date is in the past but are still open."""
    if df is None or df.empty or "closedate" not in df.columns:
        return pd.DataFrame()

    now = now or datetime.now()
    closed_mask = to_bool_series(df, "hs_is_closed")
    dates = pd.to_datetime(df["closedate"], errors="coerce", utc=True)
    now_ts = pd.Timestamp(now, tz="UTC")
    past_due = dates < now_ts
    # Not closed (or hs_is_closed missing) AND past-due close date.
    still_open = ~closed_mask if len(closed_mask) == len(df) else pd.Series(
        [True] * len(df), index=df.index
    )
    mask = past_due.fillna(False) & still_open
    return df[mask]


def find_zero_engagement_deals(
    deal_df: pd.DataFrame, deal_extractor: DealExtractor
) -> pd.DataFrame:
    """Return deals with no associated meetings/calls/emails/notes.

    Uses the deal extractor's ``get_associated_ids`` (which must be called
    on the source-type extractor) to look up engagements per type, then
    marks a deal as zero-engagement if every engagement type returned an
    empty list.
    """
    if deal_df is None or deal_df.empty or "id" not in deal_df.columns:
        return pd.DataFrame()

    deal_ids = deal_df["id"].astype(str).tolist()
    engagement_counts: dict[str, int] = {d: 0 for d in deal_ids}

    for eng_type in ENGAGEMENT_TYPES:
        try:
            mapping = deal_extractor.get_associated_ids(deal_ids, eng_type)
        except Exception:
            # Association type may not exist on this portal (e.g. notes).
            continue
        for deal_id, linked in mapping.items():
            engagement_counts[str(deal_id)] = engagement_counts.get(
                str(deal_id), 0
            ) + len(linked or [])

    ghost_ids = {d for d, count in engagement_counts.items() if count == 0}
    return deal_df[deal_df["id"].astype(str).isin(ghost_ids)]
