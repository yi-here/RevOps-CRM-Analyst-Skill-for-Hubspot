"""Forecast metrics — weighted pipeline, forecast categories."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.schema.models import CRMSchema


def weighted_pipeline(deal_extractor: DealExtractor, schema: CRMSchema) -> dict:
    """Calculate weighted pipeline value using stage probabilities."""
    df = deal_extractor.get_open_deals()
    if df.empty:
        return {"weighted_value": 0.0, "unweighted_value": 0.0, "deal_count": 0}

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)

    # Build stage → probability map
    prob_map = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                prob_map[s.stage_id] = s.probability / 100 if s.probability > 1 else s.probability

    df["probability"] = df["dealstage"].map(prob_map).fillna(0.5)
    df["weighted_amount"] = df["amount"] * df["probability"]

    return {
        "weighted_value": df["weighted_amount"].sum(),
        "unweighted_value": df["amount"].sum(),
        "deal_count": len(df),
    }


def forecast_by_category(deal_extractor: DealExtractor) -> pd.DataFrame:
    """Break down pipeline by HubSpot forecast category."""
    df = deal_extractor.get_open_deals(
        properties=["amount", "hs_forecast_category", "dealstage", "dealname"]
    )
    if df.empty:
        return pd.DataFrame()

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)

    category_col = "hs_forecast_category"
    if category_col not in df.columns:
        return pd.DataFrame()

    return df.groupby(category_col).agg(
        total_value=("amount", "sum"),
        deal_count=("id", "count"),
        avg_deal_size=("amount", "mean"),
    ).reset_index().sort_values("total_value", ascending=False)
