"""Report orchestrator — coordinates extraction, metrics, and formatting."""

from __future__ import annotations

from datetime import datetime, timedelta

from hubspot_revops.client import HubSpotClient
from hubspot_revops.extractors.activities import ActivityExtractor
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.contacts import ContactExtractor
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics import activity, conversion, forecast, pipeline, revenue, team
from hubspot_revops.reports.templates import (
    format_executive_summary,
    format_funnel_report,
    format_pipeline_report,
    format_rep_scorecard,
    format_revenue_report,
)
from hubspot_revops.schema.cache import get_or_discover_schema
from hubspot_revops.schema.models import CRMSchema


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

    def executive_summary(self, time_range: TimeRange | None = None) -> str:
        """Generate a full executive summary report."""
        tr = time_range or self._default_time_range()
        data = {
            "pipeline": pipeline.total_pipeline_value(self.deal_extractor),
            "win_rate": pipeline.win_rate(self.deal_extractor, tr),
            "avg_deal_size": pipeline.avg_deal_size(self.deal_extractor, tr),
            "velocity": pipeline.pipeline_velocity(self.deal_extractor, tr),
            "revenue": revenue.closed_revenue(self.deal_extractor, tr),
            "weighted": forecast.weighted_pipeline(self.deal_extractor, self.schema),
        }
        return format_executive_summary(data, tr)

    def pipeline_report(self, time_range: TimeRange | None = None) -> str:
        """Generate a pipeline analysis report."""
        tr = time_range or self._default_time_range()
        data = {
            "total": pipeline.total_pipeline_value(self.deal_extractor),
            "by_stage": pipeline.pipeline_by_stage(self.deal_extractor, self.schema),
            "win_rate": pipeline.win_rate(self.deal_extractor, tr),
            "velocity": pipeline.pipeline_velocity(self.deal_extractor, tr),
            "cycle": pipeline.sales_cycle_length(self.deal_extractor, tr),
        }
        return format_pipeline_report(data, tr)

    def revenue_report(self, time_range: TimeRange | None = None) -> str:
        """Generate a revenue report."""
        tr = time_range or self._default_time_range()
        data = {
            "closed": revenue.closed_revenue(self.deal_extractor, tr),
            "by_owner": revenue.revenue_by_owner(self.deal_extractor, tr, self.schema.owners),
            "mrr_arr": revenue.mrr_arr_from_deals(self.deal_extractor, tr),
        }
        return format_revenue_report(data, tr)

    def funnel_report(self, time_range: TimeRange | None = None) -> str:
        """Generate a funnel/conversion report."""
        tr = time_range or self._default_time_range()
        data = {
            "funnel": conversion.funnel_conversion_rates(self.contact_extractor, tr),
            "sources": conversion.lead_source_breakdown(self.contact_extractor, tr),
        }
        return format_funnel_report(data, tr)

    def rep_scorecard_report(self, time_range: TimeRange | None = None) -> str:
        """Generate a rep performance scorecard."""
        tr = time_range or self._default_time_range()
        scorecard = team.rep_scorecard(self.deal_extractor, tr, self.schema.owners)
        return format_rep_scorecard(scorecard, tr)

    def activity_report(self, time_range: TimeRange | None = None) -> str:
        """Generate an activity/engagement report."""
        tr = time_range or self._default_time_range()
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

        if not by_owner.empty:
            lines.append("## By Rep\n")
            pivot = by_owner.pivot_table(
                index="owner_name", columns="activity_type", values="count", fill_value=0
            )
            lines.append(pivot.to_markdown())

        return "\n".join(lines)
