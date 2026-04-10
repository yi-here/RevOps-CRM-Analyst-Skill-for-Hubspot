"""Monthly forecast bucket analysis — Commit / Highly Likely / Best Case.

Buckets open deals closing in the current calendar month into three
categories using the rules from the task:

- **Commit**: stage probability ≥ 0.8, OR HubSpot forecast category is
  ``"commit"``, OR the stage label contains "contract sent" and
  ``hs_next_step`` is non-empty (i.e. there is an actionable note).
- **Highly Likely**: 0.4 ≤ probability < 0.8, OR the deal is in a
  late-stage bucket with a recently modified ``hs_next_step``.
- **Best Case**: probability < 0.4.

Multi-currency handling: totals are never mixed across currencies.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import FORECAST_BUCKET_PROPERTIES, DealExtractor
from hubspot_revops.metrics._utils import to_numeric_series
from hubspot_revops.schema.models import CRMSchema, Owner

BUCKETS = ["Commit", "Highly Likely", "Best Case"]
DEFAULT_CURRENCY = "USD"


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _build_probability_map(schema: CRMSchema) -> dict[str, float]:
    """Same pattern as forecast.py: stage_id → probability in [0, 1]."""
    prob_map: dict[str, float] = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                prob = s.probability / 100 if s.probability > 1 else s.probability
                prob_map[s.stage_id] = float(prob)
    return prob_map


def _build_stage_label_map(schema: CRMSchema) -> dict[str, str]:
    label_map: dict[str, str] = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                label_map[s.stage_id] = s.label.lower()
    return label_map


def _current_month_range(now: datetime | None = None) -> TimeRange:
    now = now or datetime.now()
    start = datetime(now.year, now.month, 1)
    if now.month == 12:
        end = datetime(now.year + 1, 1, 1)
    else:
        end = datetime(now.year, now.month + 1, 1)
    return TimeRange(start=start, end=end)


def _assign_bucket(row: pd.Series) -> str:
    prob = row.get("probability", 0.0) or 0.0
    forecast_cat = str(row.get("hs_forecast_category", "") or "").lower()
    stage_label = str(row.get("stage_label", "") or "")
    next_step = str(row.get("hs_next_step", "") or "").strip()

    # Commit
    if prob >= 0.8:
        return "Commit"
    if forecast_cat == "commit":
        return "Commit"
    if "contract sent" in stage_label and next_step:
        return "Commit"

    # Highly Likely
    if 0.4 <= prob < 0.8:
        return "Highly Likely"
    # Late-stage (prob >= 0.5) with an active next step
    if prob >= 0.5 and next_step:
        return "Highly Likely"

    # Best case fallback
    return "Best Case"


def month_forecast_buckets(
    deal_extractor: DealExtractor,
    schema: CRMSchema,
    owners: dict[str, Owner] | None = None,
    pipeline_filter: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Compute per-rep × bucket × currency subtotals for the current month.

    Returns a dict with:
        ``period``: TimeRange of the current calendar month
        ``per_rep``: DataFrame columns [rep_name, currency, bucket, value, deals]
        ``totals_by_bucket``: DataFrame [bucket, currency, value, deals]
        ``currencies``: sorted list of distinct currency codes
    """
    tr = _current_month_range(now)
    df = _filter_pipeline(
        deal_extractor.search_in_time_range(
            time_range=tr,
            date_property="closedate",
            additional_filters=[
                {"propertyName": "hs_is_closed", "operator": "EQ", "value": "false"},
            ],
            properties=FORECAST_BUCKET_PROPERTIES,
        ),
        pipeline_filter,
    )
    empty_payload = {
        "period": tr,
        "per_rep": pd.DataFrame(),
        "totals_by_bucket": pd.DataFrame(),
        "currencies": [],
    }
    if df.empty:
        return empty_payload

    df = df.copy()
    df["amount"] = to_numeric_series(df, "amount")

    prob_map = _build_probability_map(schema)
    stage_label_map = _build_stage_label_map(schema)
    df["probability"] = df.get("dealstage", pd.Series([], dtype=str)).map(prob_map).fillna(0.0)
    df["stage_label"] = df.get("dealstage", pd.Series([], dtype=str)).map(stage_label_map).fillna("")

    # Currency + rep name normalization.
    if "deal_currency_code" in df.columns:
        df["currency"] = df["deal_currency_code"].fillna(DEFAULT_CURRENCY).replace("", DEFAULT_CURRENCY)
    else:
        df["currency"] = DEFAULT_CURRENCY
    owners = owners or {}
    if "hubspot_owner_id" not in df.columns:
        df["hubspot_owner_id"] = ""
    df["rep_name"] = df["hubspot_owner_id"].map(
        lambda oid: owners[oid].full_name if oid in owners else (oid or "Unassigned")
    )

    df["bucket"] = df.apply(_assign_bucket, axis=1)

    per_rep = df.groupby(["rep_name", "currency", "bucket"]).agg(
        value=("amount", "sum"),
        deals=("id", "count"),
    ).reset_index()
    per_rep = per_rep.sort_values(["currency", "rep_name", "bucket"])

    totals = df.groupby(["bucket", "currency"]).agg(
        value=("amount", "sum"),
        deals=("id", "count"),
    ).reset_index()

    return {
        "period": tr,
        "per_rep": per_rep,
        "totals_by_bucket": totals,
        "currencies": sorted(df["currency"].unique().tolist()),
    }
