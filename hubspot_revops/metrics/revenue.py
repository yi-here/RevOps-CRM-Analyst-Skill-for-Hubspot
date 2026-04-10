"""Revenue metrics — closed revenue, MRR/ARR, expansion, churn, NRR."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_numeric_series


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def closed_revenue(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Total closed-won revenue in a period."""
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, won_only=True), pipeline_filter
    )
    if won.empty:
        return {"total_revenue": 0.0, "deal_count": 0}

    won["amount"] = to_numeric_series(won, "amount")
    return {
        "total_revenue": won["amount"].sum(),
        "deal_count": len(won),
        "avg_deal_size": won["amount"].mean(),
        "max_deal": won["amount"].max(),
        "min_deal": won["amount"].min(),
    }


def revenue_by_owner(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    owners: dict,
    pipeline_filter: str | None = None,
) -> pd.DataFrame:
    """Revenue grouped by deal owner."""
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, won_only=True), pipeline_filter
    )
    if won.empty:
        return pd.DataFrame()

    won["amount"] = to_numeric_series(won, "amount")
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

    won["amount"] = to_numeric_series(won, "amount")
    return won.groupby("pipeline").agg(
        total_revenue=("amount", "sum"),
        deal_count=("id", "count"),
    ).reset_index().sort_values("total_revenue", ascending=False)


def mrr_arr_from_deals(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Extract MRR/ARR from deal properties (if populated)."""
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(
            time_range,
            won_only=True,
            properties=["amount", "hs_mrr", "hs_arr", "hs_acv", "closedate", "pipeline"],
        ),
        pipeline_filter,
    )
    if won.empty:
        return {"mrr": 0.0, "arr": 0.0}

    for col in ["hs_mrr", "hs_arr"]:
        won[col] = to_numeric_series(won, col)

    return {
        "mrr": won["hs_mrr"].sum(),
        "arr": won["hs_arr"].sum(),
        "deal_count": len(won),
    }
