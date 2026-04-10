"""Pipeline metrics — value, velocity, stage conversion, coverage."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import (
    DEFAULT_CURRENCY,
    attach_currency,
    pick_primary_currency,
    to_bool_series,
    to_numeric_series,
)
from hubspot_revops.schema.models import CRMSchema


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def total_pipeline_value(
    deal_extractor: DealExtractor, pipeline_filter: str | None = None
) -> dict:
    """Calculate total open pipeline value, grouped by currency.

    Summing ``amount`` across currencies produced the same class of bug
    as the revenue report — a ¥1M open deal was counted as $1M on the
    top line. Return a ``by_currency`` dict and expose the primary
    currency (highest deal count) at the top level for back-compat.
    Deal COUNTS are currency-agnostic (3 deals is 3 deals) so
    ``total_deals`` still reflects the total across every currency;
    only VALUE fields are primary-currency only.
    """
    df = _filter_pipeline(deal_extractor.get_open_deals(), pipeline_filter)
    empty_payload = {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "total_deals": 0,
        "total_value": 0.0,
        "avg_deal_size": 0.0,
    }
    if df.empty:
        return empty_payload

    df = attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    by_currency: dict[str, dict] = {}
    for code, group in df.groupby("currency"):
        amounts = group["amount"]
        by_currency[str(code)] = {
            "deal_count": int(len(group)),
            "total_value": float(amounts.sum()),
            "avg_deal_size": float(amounts.mean()) if len(group) else 0.0,
        }

    primary = pick_primary_currency(by_currency)
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        # Count is universal; value + avg are primary-currency only.
        "total_deals": int(len(df)),
        "total_value": primary_stats["total_value"],
        "avg_deal_size": primary_stats["avg_deal_size"],
    }


def pipeline_by_stage(
    deal_extractor: DealExtractor,
    schema: CRMSchema,
    pipeline_filter: str | None = None,
) -> pd.DataFrame:
    """Break down open pipeline by stage, pipeline-aware and currency-aware.

    Adds ``currency`` to the group key so a ¥ Japan stage and a $
    enterprise stage never share a row. The output DataFrame gains a
    ``currency`` column which the template uses to render a currency
    prefix on every stage row.
    """
    df = _filter_pipeline(deal_extractor.get_open_deals(), pipeline_filter)
    if df.empty:
        return pd.DataFrame()

    df = attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    # Build a (pipeline_id, stage_id) -> (stage_label, pipeline_label) map.
    # Stages can share labels across pipelines ("Qualified" in both Sales and
    # Japan), so we disambiguate by joining on both keys and include the
    # pipeline label in the output row to keep them distinct.
    stage_info: dict[tuple[str, str], tuple[str, str]] = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                stage_info[(pl.pipeline_id, s.stage_id)] = (s.label, pl.label)

    base_group_cols = ["pipeline", "dealstage"] if "pipeline" in df.columns else ["dealstage"]
    group_cols = base_group_cols + ["currency"]
    grouped = df.groupby(group_cols).agg(
        deal_count=("id", "count"),
        total_value=("amount", "sum"),
        avg_value=("amount", "mean"),
    ).reset_index()

    def _lookup_label(row: pd.Series) -> str:
        key = (row.get("pipeline", ""), row["dealstage"])
        label, pl_label = stage_info.get(key, (row["dealstage"], ""))
        return f"{label} ({pl_label})" if pl_label else label

    def _lookup_pipeline_label(row: pd.Series) -> str:
        key = (row.get("pipeline", ""), row["dealstage"])
        _, pl_label = stage_info.get(key, ("", ""))
        return pl_label

    grouped["stage_label"] = grouped.apply(_lookup_label, axis=1)
    grouped["pipeline_label"] = grouped.apply(_lookup_pipeline_label, axis=1)
    return grouped.sort_values(
        ["currency", "total_value"], ascending=[True, False]
    ).reset_index(drop=True)


def win_rate(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate win rate for deals closed in a time period."""
    closed = _filter_pipeline(deal_extractor.get_closed_deals(time_range), pipeline_filter)
    if closed.empty:
        return {"win_rate": 0.0, "won": 0, "lost": 0, "total_closed": 0}

    won_mask = to_bool_series(closed, "hs_is_closed_won")
    won = closed[won_mask]
    lost_count = len(closed) - len(won)
    rate = len(won) / len(closed) if len(closed) > 0 else 0

    return {
        "win_rate": round(rate * 100, 1),
        "won": len(won),
        "lost": lost_count,
        "total_closed": len(closed),
    }


def avg_deal_size(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate average deal size for won deals in a period, per currency.

    Averaging across currencies — e.g. ``mean([$30k, $40k, ¥1M])`` —
    produces a nonsense number. Bucket by currency and expose
    primary-currency values at the top level (matches the revenue and
    total_pipeline_value shape).
    """
    # Mirror the team / revenue pattern: fetch all closed deals and
    # filter wins in Python so we agree with revenue on exactly which
    # deals count as "won" (see the capitalized-boolean regression
    # test in test_bug_fixes.py).
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range), pipeline_filter
    )
    empty_payload = {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "avg_deal_size": 0.0,
        "total_revenue": 0.0,
        "deal_count": 0,
    }
    if closed.empty:
        return empty_payload
    won_mask = to_bool_series(closed, "hs_is_closed_won")
    won = closed[won_mask].copy()
    if won.empty:
        return empty_payload

    won = attach_currency(won)
    won["amount"] = to_numeric_series(won, "amount")

    by_currency: dict[str, dict] = {}
    for code, group in won.groupby("currency"):
        amounts = group["amount"]
        by_currency[str(code)] = {
            "deal_count": int(len(group)),
            "avg_deal_size": float(amounts.mean()) if len(group) else 0.0,
            "total_revenue": float(amounts.sum()),
        }

    primary = pick_primary_currency(by_currency)
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "avg_deal_size": primary_stats["avg_deal_size"],
        "total_revenue": primary_stats["total_revenue"],
        "deal_count": int(len(won)),
    }


def sales_cycle_length(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate average sales cycle length for won deals."""
    won = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, won_only=True), pipeline_filter
    )
    if won.empty:
        return {"avg_days": 0, "median_days": 0, "deal_count": 0}

    won["createdate"] = pd.to_datetime(won["createdate"], errors="coerce")
    won["closedate"] = pd.to_datetime(won["closedate"], errors="coerce")
    won["cycle_days"] = (won["closedate"] - won["createdate"]).dt.days

    valid = won.dropna(subset=["cycle_days"])
    return {
        "avg_days": valid["cycle_days"].mean(),
        "median_days": valid["cycle_days"].median(),
        "deal_count": len(valid),
    }


def pipeline_velocity(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate pipeline velocity = (deals × win_rate × avg_size) / cycle.

    Velocity is denominated in the **primary currency** only — mixing
    ``avg_size`` from USD wins with a deal count spanning JPY + USD
    produces a meaningless number. We look up the primary currency of
    the open pipeline and use that currency's deal count + avg_size to
    compute a single velocity figure, tagged with the currency code so
    templates can render it with the right symbol.
    """
    wr = win_rate(deal_extractor, time_range, pipeline_filter=pipeline_filter)
    ads = avg_deal_size(deal_extractor, time_range, pipeline_filter=pipeline_filter)
    scl = sales_cycle_length(deal_extractor, time_range, pipeline_filter=pipeline_filter)

    open_pipeline = total_pipeline_value(deal_extractor, pipeline_filter=pipeline_filter)
    primary = open_pipeline.get("primary_currency", DEFAULT_CURRENCY)
    open_by_currency = open_pipeline.get("by_currency", {}) or {}
    open_primary_stats = open_by_currency.get(primary, {})
    num_deals = int(open_primary_stats.get("deal_count", 0))

    # Pull avg_size from the SAME currency in the won deals, not the
    # won-side primary currency (which may differ when a rep has
    # lots of small JPY closes and a few big USD opens).
    won_by_currency = ads.get("by_currency", {}) or {}
    won_primary_stats = won_by_currency.get(primary, {})
    avg_size = float(won_primary_stats.get("avg_deal_size", 0.0))

    win_pct = wr["win_rate"] / 100
    avg_cycle = scl["avg_days"] if scl["avg_days"] > 0 else 1
    velocity = (num_deals * win_pct * avg_size) / avg_cycle

    return {
        "velocity_per_day": velocity,
        "velocity_per_month": velocity * 30,
        "open_deals": num_deals,
        "win_rate": wr["win_rate"],
        "avg_deal_size": avg_size,
        "avg_cycle_days": scl["avg_days"],
        "currency": primary,
    }
