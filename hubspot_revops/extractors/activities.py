"""Engagement/activity extraction — calls, emails, meetings, notes, tasks."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange

ENGAGEMENT_TYPES = ["calls", "emails", "meetings", "notes", "tasks"]

ENGAGEMENT_PROPERTIES = {
    "calls": ["hs_call_direction", "hs_call_duration", "hs_call_status", "hubspot_owner_id", "hs_createdate"],
    "emails": ["hs_email_direction", "hs_email_status", "hubspot_owner_id", "hs_createdate"],
    "meetings": ["hs_meeting_outcome", "hs_meeting_start_time", "hs_meeting_end_time", "hubspot_owner_id", "hs_createdate"],
    "notes": ["hubspot_owner_id", "hs_createdate"],
    "tasks": ["hs_task_status", "hs_task_priority", "hubspot_owner_id", "hs_createdate"],
}


class ActivityExtractor(BaseExtractor):
    """Extract engagement activities from HubSpot."""

    def get_activities(
        self, activity_type: str, time_range: TimeRange, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch activities of a specific type within a time range."""
        self.object_type = activity_type
        props = properties or ENGAGEMENT_PROPERTIES.get(activity_type, ["hubspot_owner_id", "hs_createdate"])
        return self.search_in_time_range(
            time_range=time_range,
            date_property="hs_createdate",
            properties=props,
        )

    def get_all_activities(self, time_range: TimeRange) -> dict[str, pd.DataFrame]:
        """Fetch all engagement types within a time range."""
        results = {}
        for eng_type in ENGAGEMENT_TYPES:
            try:
                df = self.get_activities(eng_type, time_range)
                if not df.empty:
                    results[eng_type] = df
            except Exception:
                continue
        return results
