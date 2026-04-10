"""Revenue metrics — closed revenue, MRR/ARR, expansion, churn, NRR."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_numeric_series

DEFAULT_CURRENCY = "USD"


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _attach_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Add a normalized ``currency`` column, defaulting to USD."""
    if df.empty:
        return df
    df = df.copy()
    if "deal_currency_code" in df.columns:
        df["currency"] = (
            df["deal_currency_code"]
            .fillna(DEFAULT_CURRENCY)
            .replace("", DEFAULT_CURRENCY)
        )
    else:
        df["currency"] = DEFAULT_CURRENCY
    return df


def closed_revenue(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Total closed-won revenue in a period, grouped by currency.

    Returns a payload shaped as::

        {
            "by_currency": {
                "USD": {"total_revenue": ..., "deal_count": ..., "avg_deal_size": ...,
                         "max_deal": ..., "min_deal": ...},
                "JPY": {...},
            },
            "primary_currency": "USD",  # highest deal count
            "total_deals": int,
            # Back-compat / primary-currency convenience fields:
            "total_revenue": float,
            "deal_count": int,
            "avg_deal_size": float,
            "max_deal": float,
            "min_deal": float,
        }

    Mixing JPY and USD into a single total used to silently inflate the
    revenue report — a ¥990K deal was being counted as $990K. Every
    caller now sees per-currency subtotals; the back-compat top-level
    fields reflect only the primary (highest-count) currency.
    """
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, won_only=True), pipeline_filter
    )
    empty_payload = {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "total_deals": 0,
        "total_revenue": 0.0,
        "deal_count": 0,
    }
    if won.empty:
        return empty_payload

    won = _attach_currency(won)
    won["amount"] = to_numeric_series(won, "amount")

    by_currency: dict[str, dict] = {}
    for code, group in won.groupby("currency"):
        amounts = group["amount"]
        by_currency[str(code)] = {
            "total_revenue": float(amounts.sum()),
            "deal_count": int(len(group)),
            "avg_deal_size": float(amounts.mean()) if len(group) else 0.0,
            "max_deal": float(amounts.max()) if len(group) else 0.0,
            "min_deal": float(amounts.min()) if len(group) else 0.0,
        }

    # Primary currency = whichever has the most deals. Ties are broken
    # alphabetically for determinism.
    primary = max(by_currency.items(), key=lambda kv: (kv[1]["deal_count"], kv[0]))[0]
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "total_deals": int(len(won)),
        "total_revenue": primary_stats["total_revenue"],
        "deal_count": primary_stats["deal_count"],
        "avg_deal_size": primary_stats["avg_deal_size"],
        "max_deal": primary_stats["max_deal"],
        "min_deal": primary_stats["min_deal"],
    }


def revenue_by_owner(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    owners: dict,
    pipeline_filter: str | None = None,
) -> pd.DataFrame:
    """Revenue grouped by deal owner AND currency.

    Produces one row per ``(owner, currency)`` pair, matching the team
    scorecard shape. Aggregating without currency grouping would repeat
    the JPY-in-USD bug at the rep level.
    """
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, won_only=True), pipeline_filter
    )
    if won.empty:
        return pd.DataFrame()

    won = _attach_currency(won)
    won["amount"] = to_numeric_series(won, "amount")
    grouped = won.groupby(["hubspot_owner_id", "currency"]).agg(
        total_revenue=("amount", "sum"),
        deal_count=("id", "count"),
        avg_deal_size=("amount", "mean"),
    ).reset_index()

    grouped["owner_name"] = grouped["hubspot_owner_id"].map(
        lambda oid: owners.get(oid, type("O", (), {"full_name": oid})).full_name
    )
    return grouped.sort_values(
        ["owner_name", "deal_count"], ascending=[True, False]
    ).reset_index(drop=True)


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
