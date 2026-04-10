"""Meeting history analysis — meetings per won vs. lost deal, effort sinks.

Produces:

- **per_rep**: avg meetings per won deal vs. lost deal, per rep
- **effort_sinks**: top deals with many meetings but still lost
- **time_to_close**: median days from first meeting to close, by outcome

Uses ``deal_extractor.get_associated_ids(deal_ids, "meetings")`` to map
deals → meeting IDs. ``BaseExtractor.get_associated_ids`` requires the
**source-type** extractor (``deals``), not the activity extractor — a
subtle API detail that is easy to get backwards.

Note on the ``ActivityExtractor.object_type`` mutation anti-pattern: each
call to ``ActivityExtractor.get_activities`` overwrites ``self.object_type``.
We fetch meetings once at the start of the function and never rely on the
extractor's state afterwards to avoid bleeding.
"""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.activities import ActivityExtractor
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import to_bool_series, to_numeric_series
from hubspot_revops.schema.models import Owner


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def meeting_history(
    deal_extractor: DealExtractor,
    activity_extractor: ActivityExtractor,
    time_range: TimeRange,
    owners: dict[str, Owner] | None = None,
    pipeline_filter: str | None = None,
    effort_sink_top_n: int = 10,
) -> dict:
    """Analyze meeting counts per closed deal.

    Returns a dict with:
        ``per_rep``: DataFrame of per-rep avg meetings won vs lost
        ``effort_sinks``: top-N lost deals by meeting count
        ``time_to_close``: dict with median_days_won, median_days_lost
        ``total_meetings``: int
        ``closed_deals_analyzed``: int
    """
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range), pipeline_filter
    )
    empty_payload = {
        "per_rep": pd.DataFrame(),
        "effort_sinks": pd.DataFrame(),
        "time_to_close": {"median_days_won": 0.0, "median_days_lost": 0.0},
        "total_meetings": 0,
        "closed_deals_analyzed": 0,
    }
    if closed.empty:
        return empty_payload

    closed = closed.copy()
    closed["amount"] = to_numeric_series(closed, "amount")
    closed["is_won"] = to_bool_series(closed, "hs_is_closed_won")

    deal_ids = closed["id"].astype(str).tolist()

    # Deal → meeting ID map. ``get_associated_ids`` uses the caller
    # extractor's ``object_type`` as the source, so this MUST be the deal
    # extractor.
    try:
        deal_to_meetings = deal_extractor.get_associated_ids(deal_ids, "meetings")
    except Exception:
        deal_to_meetings = {}

    meeting_counts = {
        str(d): len(deal_to_meetings.get(d, []) or []) for d in deal_ids
    }
    closed["meeting_count"] = closed["id"].astype(str).map(meeting_counts).fillna(0).astype(int)

    # Fetch meeting timestamps once so we can compute first-meeting → close.
    try:
        meetings_df = activity_extractor.get_activities("meetings", time_range)
    except Exception:
        meetings_df = pd.DataFrame()

    first_meeting_by_deal: dict[str, pd.Timestamp] = {}
    if not meetings_df.empty and "hs_meeting_start_time" in meetings_df.columns:
        meetings_df = meetings_df.copy()
        meetings_df["start"] = pd.to_datetime(
            meetings_df["hs_meeting_start_time"], errors="coerce", utc=True
        )
        # Invert deal_to_meetings: meeting_id → deal_id
        meeting_to_deal: dict[str, str] = {}
        for d, mids in deal_to_meetings.items():
            for mid in mids or []:
                meeting_to_deal[str(mid)] = str(d)
        if "id" in meetings_df.columns:
            meetings_df["deal_id"] = meetings_df["id"].astype(str).map(meeting_to_deal)
            valid = meetings_df.dropna(subset=["deal_id", "start"])
            if not valid.empty:
                first_meeting_by_deal = valid.groupby("deal_id")["start"].min().to_dict()

    closed["closedate_ts"] = pd.to_datetime(closed["closedate"], errors="coerce", utc=True)
    closed["first_meeting_ts"] = closed["id"].astype(str).map(first_meeting_by_deal)
    closed["days_first_meeting_to_close"] = (
        (closed["closedate_ts"] - closed["first_meeting_ts"]).dt.total_seconds() / 86400
    )

    # Per-rep avg meeting counts by outcome.
    owners = owners or {}
    if "hubspot_owner_id" not in closed.columns:
        closed["hubspot_owner_id"] = ""
    closed["rep_name"] = closed["hubspot_owner_id"].map(
        lambda oid: owners[oid].full_name if oid in owners else (oid or "Unassigned")
    )
    won = closed[closed["is_won"]]
    lost = closed[~closed["is_won"]]

    def _per_rep_avg(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["rep_name", f"avg_meetings_{label}", f"{label}_deals"])
        return df.groupby("rep_name").agg(
            **{
                f"avg_meetings_{label}": ("meeting_count", "mean"),
                f"{label}_deals": ("id", "count"),
            }
        ).reset_index()

    per_rep_won = _per_rep_avg(won, "won")
    per_rep_lost = _per_rep_avg(lost, "lost")
    per_rep = per_rep_won.merge(per_rep_lost, on="rep_name", how="outer").fillna(0)

    # Effort sinks: lost deals with the most meetings.
    effort_sinks = lost.sort_values("meeting_count", ascending=False).head(effort_sink_top_n)
    effort_sinks = effort_sinks[
        [c for c in ["dealname", "rep_name", "amount", "meeting_count", "closedate"] if c in effort_sinks.columns]
    ]

    median_won = (
        float(won["days_first_meeting_to_close"].dropna().median()) if not won.empty else 0.0
    )
    median_lost = (
        float(lost["days_first_meeting_to_close"].dropna().median()) if not lost.empty else 0.0
    )
    if pd.isna(median_won):
        median_won = 0.0
    if pd.isna(median_lost):
        median_lost = 0.0

    return {
        "per_rep": per_rep,
        "effort_sinks": effort_sinks.reset_index(drop=True),
        "time_to_close": {
            "median_days_won": median_won,
            "median_days_lost": median_lost,
        },
        "total_meetings": int(closed["meeting_count"].sum()),
        "closed_deals_analyzed": int(len(closed)),
    }
