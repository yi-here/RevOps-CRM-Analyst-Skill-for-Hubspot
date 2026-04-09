"""Revenue metrics — closed revenue, MRR/ARR, expansion, churn, NRR."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor


def closed_revenue(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Total closed-won revenue in a period."""
    won = deal_extractor.get_closed_deals(time_range, won_only=True)
    if won.empty:
        return {"total_revenue": 0.0, "deal_count": 0}

    won["amount"] = pd.to_numeric(won.get("amount", 0), errors="coerce").fillna(0)
    return {
        "total_revenue": won["amount"].sum(),
        "deal_count": len(won),
        "avg_deal_size": won["amount"].mean(),
        "max_deal": won["amount"].max(),
        "min_deal": won["amount"].min(),
    }


def revenue_by_owner(deal_extractor: DealExtractor, time_range: TimeRange, owners: dict) -> pd.DataFrame:
    """Revenue grouped by deal owner."""
    won = deal_extractor.get_closed_deals(time_range, won_only=True)
    if won.empty:
        return pd.DataFrame()

    won["amount"] = pd.to_numeric(won.get("amount", 0), errors="coerce").fillna(0)
    grouped = won.groupby("hubspot_owner_id").agg(
        total_revenue=("amount", "sum"),
        deal_count=("id", "count"),
        avg_deal_size=("amount", "mean"),
    ).reset_index()

    grouped["owner_name"] = grouped["hubspot_owner_id"].map(
        lambda oid: owners.get(oid, type("O", (), {"full_name": oid})).full_name
    )
    return grouped.sort_values("total_revenue", ascending=False)


def revenue_by_pipeline(deal_extractor: DealExtractor, time_range: TimeRange) -> pd.DataFrame:
    """Revenue grouped by pipeline."""
    won = deal_extractor.get_closed_deals(time_range, won_only=True)
    if won.empty:
        return pd.DataFrame()

    won["amount"] = pd.to_numeric(won.get("amount", 0), errors="coerce").fillna(0)
    return won.groupby("pipeline").agg(
        total_revenue=("amount", "sum"),
        deal_count=("id", "count"),
    ).reset_index().sort_values("total_revenue", ascending=False)


def mrr_arr_from_deals(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Extract MRR/ARR from deal properties (if populated)."""
    won = deal_extractor.get_closed_deals(
        time_range, won_only=True,
        properties=["amount", "hs_mrr", "hs_arr", "hs_acv", "closedate"],
    )
    if won.empty:
        return {"mrr": 0.0, "arr": 0.0}

    for col in ["hs_mrr", "hs_arr"]:
        won[col] = pd.to_numeric(won.get(col, 0), errors="coerce").fillna(0)

    return {
        "mrr": won["hs_mrr"].sum(),
        "arr": won["hs_arr"].sum(),
        "deal_count": len(won),
    }
