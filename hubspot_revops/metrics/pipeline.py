"""Pipeline metrics — value, velocity, stage conversion, coverage."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.schema.models import CRMSchema


def total_pipeline_value(deal_extractor: DealExtractor) -> dict:
    """Calculate total open pipeline value."""
    df = deal_extractor.get_open_deals()
    if df.empty:
        return {"total_deals": 0, "total_value": 0.0}

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
    return {
        "total_deals": len(df),
        "total_value": df["amount"].sum(),
        "avg_deal_size": df["amount"].mean(),
    }


def pipeline_by_stage(deal_extractor: DealExtractor, schema: CRMSchema) -> pd.DataFrame:
    """Break down open pipeline by stage."""
    df = deal_extractor.get_open_deals()
    if df.empty:
        return pd.DataFrame()

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)

    # Map stage IDs to labels
    stage_labels = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                stage_labels[s.stage_id] = s.label

    grouped = df.groupby("dealstage").agg(
        deal_count=("id", "count"),
        total_value=("amount", "sum"),
        avg_value=("amount", "mean"),
    ).reset_index()

    grouped["stage_label"] = grouped["dealstage"].map(stage_labels).fillna(grouped["dealstage"])
    return grouped.sort_values("total_value", ascending=False)


def win_rate(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Calculate win rate for deals closed in a time period."""
    closed = deal_extractor.get_closed_deals(time_range)
    if closed.empty:
        return {"win_rate": 0.0, "won": 0, "lost": 0, "total_closed": 0}

    won = closed[closed.get("hs_is_closed_won", "false").astype(str) == "true"]
    lost_count = len(closed) - len(won)
    rate = len(won) / len(closed) if len(closed) > 0 else 0

    return {
        "win_rate": round(rate * 100, 1),
        "won": len(won),
        "lost": lost_count,
        "total_closed": len(closed),
    }


def avg_deal_size(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Calculate average deal size for won deals in a period."""
    won = deal_extractor.get_closed_deals(time_range, won_only=True)
    if won.empty:
        return {"avg_deal_size": 0.0, "total_revenue": 0.0, "deal_count": 0}

    won["amount"] = pd.to_numeric(won.get("amount", 0), errors="coerce").fillna(0)
    return {
        "avg_deal_size": won["amount"].mean(),
        "total_revenue": won["amount"].sum(),
        "deal_count": len(won),
    }


def sales_cycle_length(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Calculate average sales cycle length for won deals."""
    won = deal_extractor.get_closed_deals(time_range, won_only=True)
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


def pipeline_velocity(deal_extractor: DealExtractor, time_range: TimeRange) -> dict:
    """Calculate pipeline velocity = (deals * win_rate * avg_size) / avg_cycle."""
    wr = win_rate(deal_extractor, time_range)
    ads = avg_deal_size(deal_extractor, time_range)
    scl = sales_cycle_length(deal_extractor, time_range)

    open_pipeline = total_pipeline_value(deal_extractor)
    num_deals = open_pipeline["total_deals"]
    win_pct = wr["win_rate"] / 100
    avg_size = ads["avg_deal_size"]
    avg_cycle = scl["avg_days"] if scl["avg_days"] > 0 else 1

    velocity = (num_deals * win_pct * avg_size) / avg_cycle

    return {
        "velocity_per_day": velocity,
        "velocity_per_month": velocity * 30,
        "open_deals": num_deals,
        "win_rate": wr["win_rate"],
        "avg_deal_size": avg_size,
        "avg_cycle_days": scl["avg_days"],
    }
