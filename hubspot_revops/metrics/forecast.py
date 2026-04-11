"""Forecast metrics — weighted pipeline, forecast categories."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_numeric_series
from hubspot_revops.metrics.forecast_bucket import _normalize_probability
from hubspot_revops.schema.models import CRMSchema

DEFAULT_CURRENCY = "USD"


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _attach_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Add a normalized ``currency`` column, defaulting to USD.

    Mirrors the equivalent helpers in ``revenue`` and ``pipeline`` so
    every money metric in the skill buckets currencies consistently.
    """
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


def weighted_pipeline(
    deal_extractor: DealExtractor,
    schema: CRMSchema,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate weighted pipeline value using stage probabilities, by currency.

    Returns a payload shaped as::

        {
            "by_currency": {
                "USD": {"weighted_value": ..., "unweighted_value": ..., "deal_count": ...},
                "JPY": {...},
            },
            "primary_currency": "USD",  # highest deal count, alphabetical tiebreak
            # Back-compat flat fields (reflect primary currency only) consumed
            # by format_executive_summary's "Weighted Pipeline" row:
            "weighted_value": float,
            "unweighted_value": float,
            "deal_count": int,
        }

    Previously this summed every weighted amount across currencies,
    producing a meaningless "¥1.04M weighted pipeline" on JPY+USD
    portals. Now buckets per currency the same way
    ``total_pipeline_value`` and ``revenue.closed_revenue`` do.
    """
    df = _filter_pipeline(deal_extractor.get_open_deals(), pipeline_filter)
    empty_payload = {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "weighted_value": 0.0,
        "unweighted_value": 0.0,
        "deal_count": 0,
    }
    if df.empty:
        return empty_payload

    df = _attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    # Build stage → probability map. Share the normalization helper
    # with forecast_bucket so weighted pipeline and the Commit / Highly
    # Likely / Best Case bucketing agree on what "80% stage" means —
    # otherwise executive summary and forecast report disagree because
    # of a rounding gap.
    prob_map: dict[str, float] = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                prob_map[s.stage_id] = _normalize_probability(s.probability)

    df["probability"] = df["dealstage"].map(prob_map).fillna(0.5)
    df["weighted_amount"] = df["amount"] * df["probability"]

    by_currency: dict[str, dict] = {}
    for code, group in df.groupby("currency"):
        by_currency[str(code)] = {
            "weighted_value": float(group["weighted_amount"].sum()),
            "unweighted_value": float(group["amount"].sum()),
            "deal_count": int(len(group)),
        }

    primary = max(by_currency.items(), key=lambda kv: (kv[1]["deal_count"], kv[0]))[0]
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "weighted_value": primary_stats["weighted_value"],
        "unweighted_value": primary_stats["unweighted_value"],
        "deal_count": int(len(df)),
    }


def forecast_by_category(
    deal_extractor: DealExtractor, pipeline_filter: str | None = None
) -> pd.DataFrame:
    """Break down pipeline by HubSpot forecast category, per currency.

    Returns a DataFrame with columns::

        [hs_forecast_category, currency, total_value, deal_count, avg_deal_size]

    Sorted by currency, then total_value descending. One row per
    ``(category, currency)`` pair so JPY and USD buckets never mix —
    previously "Commit: ¥500K + $30K → $530K" was rendered as a single
    row and silently inflated the total.
    """
    df = _filter_pipeline(
        deal_extractor.get_open_deals(
            properties=[
                "amount",
                "hs_forecast_category",
                "dealstage",
                "dealname",
                "pipeline",
                "deal_currency_code",
            ]
        ),
        pipeline_filter,
    )
    if df.empty:
        return pd.DataFrame()

    category_col = "hs_forecast_category"
    if category_col not in df.columns:
        return pd.DataFrame()

    df = _attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    return df.groupby([category_col, "currency"]).agg(
        total_value=("amount", "sum"),
        deal_count=("id", "count"),
        avg_deal_size=("amount", "mean"),
    ).reset_index().sort_values(
        ["currency", "total_value"], ascending=[True, False]
    ).reset_index(drop=True)
