"""Report orchestrator — coordinates extraction, metrics, and formatting."""

from __future__ import annotations

from datetime import datetime, timedelta

from hubspot_revops.client import HubSpotClient
from hubspot_revops.extractors.activities import ActivityExtractor
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.contacts import ContactExtractor
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics import (
    activity,
    closed_lost,
    conversion,
    forecast,
    forecast_bucket,
    meeting_history,
    pipeline,
    revenue,
    team,
)
from hubspot_revops.reports.templates import (
    format_closed_lost_report,
    format_executive_summary,
    format_forecast_report,
    format_funnel_report,
    format_meeting_history_report,
    format_pipeline_report,
    format_rep_scorecard,
    format_revenue_report,
)
from hubspot_revops.schema.cache import get_or_discover_schema
from hubspot_revops.schema.models import CRMSchema
from hubspot_revops.schema.stage_ids import resolve_pipeline_id


class ReportGenerator:
    """Orchestrates metric computation and report formatting."""

    def __init__(self, client: HubSpotClient, schema: CRMSchema | None = None) -> None:
        self.client = client
        self.schema = schema or get_or_discover_schema(client)
        self.deal_extractor = DealExtractor(client)
        self.contact_extractor = ContactExtractor(client)
        self.activity_extractor = ActivityExtractor(client)

    def _default_time_range(self, days: int = 90) -> TimeRange:
        end = datetime.now()
        start = end - timedelta(days=days)
        return TimeRange(start=start, end=end)

    def _resolve_pipeline(self, name_or_id: str | None) -> str | None:
        """Translate a user-supplied label or ID to a pipeline_id.

        Returns ``None`` for ``None``/``"all"``/unknown — callers treat that
        as "all pipelines".
        """
        return resolve_pipeline_id(self.schema, name_or_id)

    def executive_summary(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a full executive summary report."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        data = {
            "pipeline": pipeline.total_pipeline_value(self.deal_extractor, pipeline_filter=pid),
            "win_rate": pipeline.win_rate(self.deal_extractor, tr, pipeline_filter=pid),
            "avg_deal_size": pipeline.avg_deal_size(self.deal_extractor, tr, pipeline_filter=pid),
            "velocity": pipeline.pipeline_velocity(self.deal_extractor, tr, pipeline_filter=pid),
            "revenue": revenue.closed_revenue(self.deal_extractor, tr, pipeline_filter=pid),
            "weighted": forecast.weighted_pipeline(
                self.deal_extractor, self.schema, pipeline_filter=pid
            ),
        }
        return format_executive_summary(data, tr)

    def pipeline_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a pipeline analysis report."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        data = {
            "total": pipeline.total_pipeline_value(self.deal_extractor, pipeline_filter=pid),
            "by_stage": pipeline.pipeline_by_stage(
                self.deal_extractor, self.schema, pipeline_filter=pid
            ),
            "win_rate": pipeline.win_rate(self.deal_extractor, tr, pipeline_filter=pid),
            "velocity": pipeline.pipeline_velocity(self.deal_extractor, tr, pipeline_filter=pid),
            "cycle": pipeline.sales_cycle_length(
                self.deal_extractor, tr, pipeline_filter=pid
            ),
        }
        return format_pipeline_report(data, tr)

    def revenue_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a revenue report."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        data = {
            "closed": revenue.closed_revenue(self.deal_extractor, tr, pipeline_filter=pid),
            "by_owner": revenue.revenue_by_owner(
                self.deal_extractor, tr, self.schema.owners, pipeline_filter=pid
            ),
            "mrr_arr": revenue.mrr_arr_from_deals(
                self.deal_extractor, tr, pipeline_filter=pid
            ),
        }
        return format_revenue_report(data, tr)

    def funnel_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a funnel/conversion report."""
        tr = time_range or self._default_time_range()
        # Funnel is contact-based and not pipeline-scoped.
        data = {
            "funnel": conversion.funnel_conversion_rates(self.contact_extractor, tr),
            "sources": conversion.lead_source_breakdown(self.contact_extractor, tr),
        }
        return format_funnel_report(data, tr)

    def rep_scorecard_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a rep performance scorecard."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        scorecard = team.rep_scorecard(
            self.deal_extractor, tr, self.schema.owners, pipeline_filter=pid
        )
        return format_rep_scorecard(scorecard, tr)

    def closed_lost_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a closed-lost analysis report."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        data = closed_lost.closed_lost_analysis(
            self.deal_extractor, tr, self.schema.owners, pipeline_filter=pid
        )
        return format_closed_lost_report(data, tr)

    def forecast_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a monthly forecast bucket report."""
        pid = self._resolve_pipeline(pipeline_id)
        data = forecast_bucket.month_forecast_buckets(
            self.deal_extractor,
            self.schema,
            owners=self.schema.owners,
            pipeline_filter=pid,
        )
        return format_forecast_report(data)

    def meetings_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate a meeting history report."""
        tr = time_range or self._default_time_range()
        pid = self._resolve_pipeline(pipeline_id)
        data = meeting_history.meeting_history(
            self.deal_extractor,
            self.activity_extractor,
            tr,
            owners=self.schema.owners,
            pipeline_filter=pid,
        )
        return format_meeting_history_report(data, tr)

    def activity_report(
        self, time_range: TimeRange | None = None, pipeline_id: str | None = None
    ) -> str:
        """Generate an activity/engagement report."""
        tr = time_range or self._default_time_range()
        # Activity report is engagement-based and not pipeline-scoped.
        summary = activity.activity_summary(self.activity_extractor, tr)
        by_owner = activity.activities_by_owner(self.activity_extractor, tr, self.schema.owners)

        lines = [
            f"# Activity Report",
            f"**Period:** {tr.start.strftime('%Y-%m-%d')} to {tr.end.strftime('%Y-%m-%d')}\n",
            "## Summary\n",
        ]
        for act_type, count in summary.items():
            if act_type != "total":
                lines.append(f"- **{act_type.title()}:** {count}")
        lines.append(f"- **Total:** {summary.get('total', 0)}\n")

        import pandas as pd
        if isinstance(by_owner, pd.DataFrame) and not by_owner.empty:
            lines.append("## By Rep\n")
            pivot = by_owner.pivot_table(
                index="owner_name", columns="activity_type", values="count", fill_value=0
            )
            lines.append(pivot.to_markdown())

        return "\n".join(lines)
