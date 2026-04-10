"""Closed-lost analysis — rep scorecard, reason breakdown, ghost deals.

This report specifically addresses the RevOps question "why are we losing
deals and how many of them never actually got worked?". It surfaces:

- **rep scorecard**: total lost, lost value, per-rep, per-currency
- **reason breakdown**: count + value per ``closed_lost_reason``,
  per-currency
- **ghost deal count**: closed-lost deals with zero associated
  engagements (currency-agnostic — counts deals, not value)
- **lost-reason coverage**: fraction of lost deals missing a reason; when
  this is below 50 % the report emits a warning banner because the
  reason breakdown is unreliable.

Multi-currency handling mirrors the revenue module: lost values are
bucketed by ``deal_currency_code`` and never summed across currencies.
A ¥990K deal in the Japan pipeline no longer inflates the USD lost
total by $990K. Back-compat fields (``rep_scorecard``,
``reason_breakdown``, ``total_lost_value``) expose the *primary*
currency — the one with the most lost deals — so single-currency
portals see identical output to the previous version.

Works across all pipelines because ``get_closed_deals`` filters on the
HubSpot-managed ``hs_is_closed`` / ``hs_is_closed_won`` booleans, which
are populated from every pipeline's stage metadata.
"""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import CLOSED_LOST_PROPERTIES, DealExtractor
from hubspot_revops.metrics._quality import find_zero_engagement_deals
from hubspot_revops.metrics._utils import to_bool_series, to_numeric_series
from hubspot_revops.schema.models import Owner

COVERAGE_WARN_THRESHOLD = 0.5
DEFAULT_CURRENCY = "USD"


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _attach_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Add a normalized ``currency`` column, defaulting to USD."""
    if df.empty:
        return df
    if "deal_currency_code" in df.columns:
        df["currency"] = (
            df["deal_currency_code"]
            .fillna(DEFAULT_CURRENCY)
            .replace("", DEFAULT_CURRENCY)
        )
    else:
        df["currency"] = DEFAULT_CURRENCY
    return df


def _build_rep_scorecard(
    group: pd.DataFrame, owners: dict[str, Owner]
) -> pd.DataFrame:
    sc = group.groupby("hubspot_owner_id").agg(
        deals_lost=("id", "count"),
        lost_value=("amount", "sum"),
        avg_lost_deal=("amount", "mean"),
    ).reset_index()
    sc["rep_name"] = sc["hubspot_owner_id"].map(
        lambda oid: owners[oid].full_name if oid in owners else (oid or "Unassigned")
    )
    return sc.sort_values("lost_value", ascending=False).reset_index(drop=True)


def _build_reason_breakdown(group: pd.DataFrame) -> pd.DataFrame:
    reason_col = "closed_lost_reason"
    reasons = group.copy()
    if reason_col not in reasons.columns:
        reasons[reason_col] = ""
    reasons[reason_col] = reasons[reason_col].fillna("").replace("", "(no reason)")
    return (
        reasons.groupby(reason_col)
        .agg(
            deals_lost=("id", "count"),
            lost_value=("amount", "sum"),
        )
        .reset_index()
        .sort_values("lost_value", ascending=False)
        .reset_index(drop=True)
    )


def _empty_payload(owners: dict[str, Owner] | None) -> dict:
    return {
        "by_currency": {},
        "primary_currency": DEFAULT_CURRENCY,
        "rep_scorecard": pd.DataFrame(),
        "reason_breakdown": pd.DataFrame(),
        "ghost_deal_count": 0,
        "lost_reason_coverage": 1.0,
        "coverage_warning": False,
        "total_lost_deals": 0,
        "total_lost_value": 0.0,
        "owners": owners or {},
    }


def closed_lost_analysis(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    owners: dict[str, Owner] | None = None,
    pipeline_filter: str | None = None,
) -> dict:
    """Compute the closed-lost report payload.

    Returns a dict with:
        ``by_currency``: dict of currency → {rep_scorecard, reason_breakdown,
            total_lost_deals, total_lost_value}
        ``primary_currency``: currency with the most lost deals
        ``rep_scorecard``: primary-currency scorecard (back-compat)
        ``reason_breakdown``: primary-currency reasons (back-compat)
        ``ghost_deal_count``: int, deals with zero associated engagements
            (counted across all currencies)
        ``lost_reason_coverage``: float in [0, 1] across all currencies
        ``coverage_warning``: bool, True when coverage below threshold
        ``total_lost_deals``: int, total across every currency
        ``total_lost_value``: float, *primary currency only* — the value
            is meaningless across currencies, so we never sum JPY + USD
            into a single total.
        ``owners``: passthrough for the template's name lookup
    """
    owners = owners or {}
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, properties=CLOSED_LOST_PROPERTIES),
        pipeline_filter,
    )
    if closed.empty:
        return _empty_payload(owners)

    won_mask = to_bool_series(closed, "hs_is_closed_won")
    lost = closed[~won_mask].copy()
    if lost.empty:
        return _empty_payload(owners)

    lost["amount"] = to_numeric_series(lost, "amount")
    if "hubspot_owner_id" not in lost.columns:
        lost["hubspot_owner_id"] = ""
    lost = _attach_currency(lost)

    # Per-currency rep scorecard + reason breakdown.
    by_currency: dict[str, dict] = {}
    for code, group in lost.groupby("currency"):
        by_currency[str(code)] = {
            "rep_scorecard": _build_rep_scorecard(group, owners),
            "reason_breakdown": _build_reason_breakdown(group),
            "total_lost_deals": int(len(group)),
            "total_lost_value": float(group["amount"].sum()),
        }

    # Primary currency = highest deal count; alphabetical tiebreak.
    primary = max(
        by_currency.items(), key=lambda kv: (kv[1]["total_lost_deals"], kv[0])
    )[0]
    primary_stats = by_currency[primary]

    # Ghost deals — currency-agnostic count across all lost deals.
    try:
        ghost_df = find_zero_engagement_deals(lost, deal_extractor)
        ghost_count = len(ghost_df)
    except Exception:
        ghost_count = 0

    # Lost-reason coverage is measured across ALL currencies; reps are
    # equally accountable for filling it in regardless of which
    # pipeline the deal lived in.
    reason_col = "closed_lost_reason"
    total = len(lost)
    if reason_col in lost.columns:
        missing = lost[reason_col].fillna("").eq("").sum()
    else:
        missing = total
    coverage = 1.0 - (missing / total) if total else 1.0

    return {
        "by_currency": by_currency,
        "primary_currency": primary,
        "rep_scorecard": primary_stats["rep_scorecard"],
        "reason_breakdown": primary_stats["reason_breakdown"],
        "ghost_deal_count": int(ghost_count),
        "lost_reason_coverage": float(coverage),
        "coverage_warning": bool(coverage < COVERAGE_WARN_THRESHOLD),
        "total_lost_deals": int(total),
        "total_lost_value": primary_stats["total_lost_value"],
        "owners": owners,
    }
