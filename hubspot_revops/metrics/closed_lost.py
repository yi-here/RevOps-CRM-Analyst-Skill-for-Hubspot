"""Closed-lost analysis — rep scorecard, reason breakdown, ghost deals.

This report specifically addresses the RevOps question "why are we losing
deals and how many of them never actually got worked?". It surfaces:

- **rep scorecard**: total lost, lost value, per-rep
- **reason breakdown**: count + value per ``closed_lost_reason``
- **ghost deal count**: closed-lost deals with zero associated engagements
- **lost-reason coverage**: fraction of lost deals missing a reason; when
  this is below 50 % the report emits a warning banner because the
  reason breakdown is unreliable.

Works across all pipelines because ``get_closed_deals`` filters on the
HubSpot-managed ``hs_is_closed`` / ``hs_is_closed_won`` booleans, which are
populated from every pipeline's stage metadata.
"""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import CLOSED_LOST_PROPERTIES, DealExtractor
from hubspot_revops.metrics._quality import find_zero_engagement_deals
from hubspot_revops.metrics._utils import to_bool_series, to_numeric_series
from hubspot_revops.schema.models import Owner

COVERAGE_WARN_THRESHOLD = 0.5


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def closed_lost_analysis(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    owners: dict[str, Owner] | None = None,
    pipeline_filter: str | None = None,
) -> dict:
    """Compute the closed-lost report payload.

    Returns a dict with:
        ``rep_scorecard``: DataFrame of per-rep loss counts/values
        ``reason_breakdown``: DataFrame grouped by ``closed_lost_reason``
        ``ghost_deal_count``: int, deals with zero associated engagements
        ``lost_reason_coverage``: float in [0, 1]
        ``coverage_warning``: bool, True when coverage below threshold
        ``total_lost_deals``: int
        ``total_lost_value``: float
        ``owners``: passthrough for the template's name lookup
    """
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range, properties=CLOSED_LOST_PROPERTIES),
        pipeline_filter,
    )
    if closed.empty:
        return {
            "rep_scorecard": pd.DataFrame(),
            "reason_breakdown": pd.DataFrame(),
            "ghost_deal_count": 0,
            "lost_reason_coverage": 1.0,
            "coverage_warning": False,
            "total_lost_deals": 0,
            "total_lost_value": 0.0,
            "owners": owners or {},
        }

    won_mask = to_bool_series(closed, "hs_is_closed_won")
    lost = closed[~won_mask].copy()
    if lost.empty:
        return {
            "rep_scorecard": pd.DataFrame(),
            "reason_breakdown": pd.DataFrame(),
            "ghost_deal_count": 0,
            "lost_reason_coverage": 1.0,
            "coverage_warning": False,
            "total_lost_deals": 0,
            "total_lost_value": 0.0,
            "owners": owners or {},
        }

    lost["amount"] = to_numeric_series(lost, "amount")

    # Rep scorecard.
    if "hubspot_owner_id" not in lost.columns:
        lost["hubspot_owner_id"] = ""
    rep_scorecard = lost.groupby("hubspot_owner_id").agg(
        deals_lost=("id", "count"),
        lost_value=("amount", "sum"),
        avg_lost_deal=("amount", "mean"),
    ).reset_index()
    owners = owners or {}
    rep_scorecard["rep_name"] = rep_scorecard["hubspot_owner_id"].map(
        lambda oid: owners[oid].full_name if oid in owners else (oid or "Unassigned")
    )
    rep_scorecard = rep_scorecard.sort_values("lost_value", ascending=False)

    # Reason breakdown.
    reason_col = "closed_lost_reason"
    if reason_col not in lost.columns:
        lost[reason_col] = ""
    reasons = lost.copy()
    reasons[reason_col] = reasons[reason_col].fillna("").replace("", "(no reason)")
    reason_breakdown = reasons.groupby(reason_col).agg(
        deals_lost=("id", "count"),
        lost_value=("amount", "sum"),
    ).reset_index().sort_values("lost_value", ascending=False)

    # Ghost deals — deals with zero engagement.
    try:
        ghost_df = find_zero_engagement_deals(lost, deal_extractor)
        ghost_count = len(ghost_df)
    except Exception:
        ghost_count = 0

    # Lost-reason coverage.
    total = len(lost)
    if reason_col in lost.columns:
        missing = lost[reason_col].fillna("").eq("").sum()
    else:
        missing = total
    coverage = 1.0 - (missing / total) if total else 1.0

    return {
        "rep_scorecard": rep_scorecard,
        "reason_breakdown": reason_breakdown,
        "ghost_deal_count": int(ghost_count),
        "lost_reason_coverage": float(coverage),
        "coverage_warning": bool(coverage < COVERAGE_WARN_THRESHOLD),
        "total_lost_deals": int(total),
        "total_lost_value": float(lost["amount"].sum()),
        "owners": owners,
    }
