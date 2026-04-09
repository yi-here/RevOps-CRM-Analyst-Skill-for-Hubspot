"""Team performance metrics — per-rep pipeline, win rate, activity."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.schema.models import Owner


def rep_scorecard(
    deal_extractor: DealExtractor, time_range: TimeRange, owners: dict[str, Owner]
) -> pd.DataFrame:
    """Generate a per-rep scorecard with key metrics."""
    # Get closed deals in period
    closed = deal_extractor.get_closed_deals(time_range)
    open_deals = deal_extractor.get_open_deals()

    if closed.empty and open_deals.empty:
        return pd.DataFrame()

    rows = []
    for owner_id, owner in owners.items():
        # Closed metrics
        rep_closed = closed[closed["hubspot_owner_id"] == owner_id] if not closed.empty else pd.DataFrame()
        rep_closed_amount = pd.to_numeric(rep_closed.get("amount", 0), errors="coerce").fillna(0)
        rep_won = rep_closed[rep_closed.get("hs_is_closed_won", "false").astype(str) == "true"] if not rep_closed.empty else pd.DataFrame()
        rep_won_amount = pd.to_numeric(rep_won.get("amount", 0), errors="coerce").fillna(0)

        # Open pipeline
        rep_open = open_deals[open_deals["hubspot_owner_id"] == owner_id] if not open_deals.empty else pd.DataFrame()
        rep_open_amount = pd.to_numeric(rep_open.get("amount", 0), errors="coerce").fillna(0)

        wr = (len(rep_won) / len(rep_closed) * 100) if len(rep_closed) > 0 else 0

        rows.append({
            "owner_id": owner_id,
            "rep_name": owner.full_name,
            "open_pipeline": rep_open_amount.sum(),
            "open_deals": len(rep_open),
            "closed_won_revenue": rep_won_amount.sum(),
            "deals_won": len(rep_won),
            "deals_closed": len(rep_closed),
            "win_rate": round(wr, 1),
            "avg_deal_size": rep_won_amount.mean() if len(rep_won) > 0 else 0,
        })

    df = pd.DataFrame(rows)
    return df.sort_values("closed_won_revenue", ascending=False) if not df.empty else df
