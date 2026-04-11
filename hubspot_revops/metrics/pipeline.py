"""Pipeline metrics — value, velocity, stage conversion, coverage."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_bool_series, to_numeric_series
from hubspot_revops.schema.models import CRMSchema

DEFAULT_CURRENCY = "USD"


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _attach_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Add a normalized ``currency`` column, defaulting to USD.

    Mirrors ``revenue._attach_currency`` so open-pipeline metrics bucket
    the same way closed-revenue metrics do — multi-currency portals must
    never sum ¥ and $ into the same scalar.
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


def _fetch_won(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None,
) -> pd.DataFrame:
    """Fetch closed deals and filter to wins *in Python*.

    Mirrors ``revenue._fetch_won``. The pipeline metrics (``avg_deal_size``,
    ``sales_cycle_length``) previously passed ``won_only=True`` to
    ``get_closed_deals``, which appends ``hs_is_closed_won EQ "true"``
    to the HubSpot Search filter — a strict, case-sensitive string
    match. The SDK sometimes returns ``hs_is_closed_won`` as
    ``"True"`` or ``"TRUE"``, in which case the API filter silently
    excludes those wins and the pipeline metrics disagreed with both
    the team scorecard (~$25K gap) and revenue (same ~$25K gap but
    with the opposite bias). Routing every win-path through the same
    Python-side ``to_bool_series`` filter eliminates the inconsistency
    so every money metric in the skill agrees on "what counts as won".
    """
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range), pipeline_filter
    )
    if closed.empty:
        return closed
    won_mask = to_bool_series(closed, "hs_is_closed_won")
    return closed[won_mask].copy()


def total_pipeline_value(
    deal_extractor: DealExtractor, pipeline_filter: str | None = None
) -> dict:
    """Calculate total open pipeline value, grouped by currency.

    Returns a payload shaped as::

        {
            "by_currency": {
                "USD": {"total_value": ..., "deal_count": ..., "avg_deal_size": ...},
                "JPY": {...},
            },
            "primary_currency": "USD",  # highest deal count, alphabetical tiebreak
            "total_deals": int,
            # Back-compat / primary-currency convenience fields consumed by
            # pipeline_velocity() and the report templates:
            "total_value": float,
            "avg_deal_size": float,
        }

    Previously this summed every open deal amount into a single scalar,
    which silently inflated multi-currency portals — a ¥990K deal was
    being counted as $990K of pipeline. Every caller now sees per-currency
    subtotals; the back-compat top-level fields reflect only the primary
    (highest-count) currency.
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

    df = _attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    by_currency: dict[str, dict] = {}
    for code, group in df.groupby("currency"):
        amounts = group["amount"]
        by_currency[str(code)] = {
            "total_value": float(amounts.sum()),
            "deal_count": int(len(group)),
            "avg_deal_size": float(amounts.mean()) if len(group) else 0.0,
        }

    # Primary currency = highest deal count. Ties are broken alphabetically
    # for determinism (matches revenue.closed_revenue's choice function).
    primary = max(by_currency.items(), key=lambda kv: (kv[1]["deal_count"], kv[0]))[0]
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "total_deals": int(len(df)),
        "total_value": primary_stats["total_value"],
        "avg_deal_size": primary_stats["avg_deal_size"],
    }


def pipeline_by_stage(
    deal_extractor: DealExtractor,
    schema: CRMSchema,
    pipeline_filter: str | None = None,
) -> pd.DataFrame:
    """Break down open pipeline by stage, pipeline-aware AND currency-aware.

    Returns one row per ``(pipeline, dealstage, currency)`` tuple so
    mixed-currency stages never collapse into a single misleading total.
    A "Proposal" stage containing a ¥1M deal and a $50K deal used to
    render as "Proposal: $525K avg" — now it produces two separate rows
    (``Proposal (Sales) / JPY / ¥1M`` and ``Proposal (Sales) / USD / $50K``).

    Stages can share labels across pipelines ("Qualified" in both Sales
    and Japan), so we disambiguate by joining on ``(pipeline_id,
    stage_id)`` and include the pipeline label in the rendered
    ``stage_label`` to keep them distinct.
    """
    df = _filter_pipeline(deal_extractor.get_open_deals(), pipeline_filter)
    if df.empty:
        return pd.DataFrame()

    df = _attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")

    # Build a (pipeline_id, stage_id) -> (stage_label, pipeline_label) map.
    stage_info: dict[tuple[str, str], tuple[str, str]] = {}
    for pipelines in schema.pipelines.values():
        for pl in pipelines:
            for s in pl.stages:
                stage_info[(pl.pipeline_id, s.stage_id)] = (s.label, pl.label)

    group_cols = (
        ["pipeline", "dealstage", "currency"]
        if "pipeline" in df.columns
        else ["dealstage", "currency"]
    )
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
    """Calculate average deal size for won deals in a period, grouped by currency.

    Returns a payload shaped as::

        {
            "by_currency": {
                "USD": {"avg_deal_size": ..., "total_revenue": ..., "deal_count": ...},
                "JPY": {...},
            },
            "primary_currency": "USD",  # highest deal count, alphabetical tiebreak
            # Back-compat / primary-currency convenience fields consumed by
            # pipeline_velocity() and the executive summary template:
            "avg_deal_size": float,
            "total_revenue": float,
            "deal_count": int,
        }

    Previously this averaged every won deal's amount regardless of
    ``deal_currency_code`` — a ¥1,000,000 JPY deal and a $50,000 USD
    deal would produce an "average" of $525,000, which is meaningless.
    Mirrors ``total_pipeline_value`` and ``revenue.closed_revenue`` so
    every money metric in the skill buckets currencies consistently.

    Uses the Python-side ``_fetch_won`` filter rather than the API-level
    ``won_only=True`` parameter; see ``_fetch_won`` for why (SDK boolean
    case-sensitivity bug that created a ~$25K gap versus team/revenue).
    """
    won = _fetch_won(deal_extractor, time_range, pipeline_filter)
    empty_payload = {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "avg_deal_size": 0.0,
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
            "avg_deal_size": float(amounts.mean()) if len(group) else 0.0,
            "total_revenue": float(amounts.sum()),
            "deal_count": int(len(group)),
        }

    # Primary currency = whichever has the most deals. Ties broken
    # alphabetically for determinism (matches total_pipeline_value and
    # revenue.closed_revenue).
    primary = max(by_currency.items(), key=lambda kv: (kv[1]["deal_count"], kv[0]))[0]
    primary_stats = by_currency[primary]
    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "avg_deal_size": primary_stats["avg_deal_size"],
        "total_revenue": primary_stats["total_revenue"],
        "deal_count": primary_stats["deal_count"],
    }


def sales_cycle_length(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    pipeline_filter: str | None = None,
) -> dict:
    """Calculate average sales cycle length for won deals.

    Uses the Python-side ``_fetch_won`` filter rather than the API-level
    ``won_only=True`` parameter to stay consistent with every other
    money metric in the skill. See ``_fetch_won`` for details.
    """
    won = _fetch_won(deal_extractor, time_range, pipeline_filter)
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
    """Calculate pipeline velocity = (deals * win_rate * avg_size) / avg_cycle."""
    wr = win_rate(deal_extractor, time_range, pipeline_filter=pipeline_filter)
    ads = avg_deal_size(deal_extractor, time_range, pipeline_filter=pipeline_filter)
    scl = sales_cycle_length(deal_extractor, time_range, pipeline_filter=pipeline_filter)

    open_pipeline = total_pipeline_value(deal_extractor, pipeline_filter=pipeline_filter)
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
