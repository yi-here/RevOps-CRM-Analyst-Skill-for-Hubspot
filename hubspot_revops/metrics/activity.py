"""Activity and engagement metrics."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.activities import ActivityExtractor
from hubspot_revops.extractors.base import TimeRange


def activity_summary(activity_extractor: ActivityExtractor, time_range: TimeRange) -> dict:
    """Summarize all engagement activity in a period."""
    all_activities = activity_extractor.get_all_activities(time_range)

    summary = {}
    total = 0
    for activity_type, df in all_activities.items():
        count = len(df)
        summary[activity_type] = count
        total += count

    summary["total"] = total
    return summary


def activities_by_owner(
    activity_extractor: ActivityExtractor, time_range: TimeRange, owners: dict
) -> pd.DataFrame:
    """Break down activities by owner across all engagement types."""
    all_activities = activity_extractor.get_all_activities(time_range)

    rows = []
    for activity_type, df in all_activities.items():
        if "hubspot_owner_id" in df.columns:
            for owner_id, group in df.groupby("hubspot_owner_id"):
                rows.append({
                    "owner_id": owner_id,
                    "owner_name": owners.get(owner_id, type("O", (), {"full_name": owner_id})).full_name,
                    "activity_type": activity_type,
                    "count": len(group),
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame()
