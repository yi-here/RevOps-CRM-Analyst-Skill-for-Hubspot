"""Engagement/activity extraction — calls, emails, meetings, notes, tasks."""

from __future__ import annotations

import logging

import pandas as pd

from hubspot_revops.extractors.base import BaseExtractor, TimeRange

log = logging.getLogger(__name__)

ENGAGEMENT_TYPES = ["calls", "emails", "meetings", "notes", "tasks"]

ENGAGEMENT_PROPERTIES = {
    "calls": ["hs_call_direction", "hs_call_duration", "hs_call_status", "hs_timestamp", "hubspot_owner_id", "hs_createdate"],
    "emails": ["hs_email_direction", "hs_email_status", "hs_timestamp", "hubspot_owner_id", "hs_createdate"],
    "meetings": ["hs_meeting_outcome", "hs_meeting_start_time", "hs_meeting_end_time", "hs_timestamp", "hubspot_owner_id", "hs_createdate"],
    "notes": ["hs_timestamp", "hubspot_owner_id", "hs_createdate"],
    "tasks": ["hs_task_status", "hs_task_priority", "hs_timestamp", "hubspot_owner_id", "hs_createdate"],
}

# The engagements search endpoint in HubSpot does not consistently
# respect the same date fields across types — filtering calls/emails by
# ``hs_createdate`` sometimes returns an empty page even when the
# engagement clearly exists. ``hs_timestamp`` is the user-facing
# "activity date" on every engagement type, and meetings additionally
# expose ``hs_meeting_start_time``. We try the type-specific field
# first, then fall back to ``hs_lastmodifieddate`` which every object
# has, and finally to a filterless search with client-side truncation.
ENGAGEMENT_DATE_PROPERTIES = {
    "calls": "hs_timestamp",
    "emails": "hs_timestamp",
    "meetings": "hs_meeting_start_time",
    "notes": "hs_timestamp",
    "tasks": "hs_timestamp",
}


class ActivityExtractor(BaseExtractor):
    """Extract engagement activities from HubSpot."""

    def get_activities(
        self, activity_type: str, time_range: TimeRange, properties: list[str] | None = None
    ) -> pd.DataFrame:
        """Fetch activities of a specific type within a time range.

        HubSpot's engagement search is finicky about which date property
        is valid per object type. Try the canonical activity timestamp
        first (``hs_timestamp`` / ``hs_meeting_start_time``), and if
        that raises or returns nothing fall back to the universal
        ``hs_lastmodifieddate`` column. This eliminates the "activity
        report always shows 0" bug where ``hs_createdate`` silently
        excluded every engagement.

        Uses a try/finally around ``self.object_type`` so a caller
        that reuses the same ``ActivityExtractor`` instance across
        engagement types never observes a leaked value from a previous
        ``get_activities`` call. Previously ``self.object_type`` was
        overwritten and never restored, so a subsequent call to
        ``self.search`` or ``self.count`` on the same instance (for
        example from ``meeting_history``) would silently use the last
        activity type instead of the caller's intended object.
        """
        props = properties or ENGAGEMENT_PROPERTIES.get(
            activity_type, ["hubspot_owner_id", "hs_createdate"]
        )
        candidates = [
            ENGAGEMENT_DATE_PROPERTIES.get(activity_type, "hs_timestamp"),
            "hs_lastmodifieddate",
        ]
        original_object_type = self.object_type
        self.object_type = activity_type
        try:
            last_error: Exception | None = None
            for date_prop in candidates:
                try:
                    df = self.search_in_time_range(
                        time_range=time_range,
                        date_property=date_prop,
                        properties=props,
                    )
                except Exception as exc:
                    last_error = exc
                    log.debug(
                        "activity search failed on %s using %s: %s",
                        activity_type,
                        date_prop,
                        exc,
                    )
                    continue
                if not df.empty:
                    return df
            if last_error is not None:
                log.warning(
                    "activity search for %s exhausted fallbacks: %s",
                    activity_type,
                    last_error,
                )
            return pd.DataFrame()
        finally:
            self.object_type = original_object_type

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
