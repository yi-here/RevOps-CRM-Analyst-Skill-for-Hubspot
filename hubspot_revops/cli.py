"""CLI entry point for the HubSpot RevOps Analyst Skill."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from hubspot_revops.client import HubSpotClient
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.nl_interface import answer_question
from hubspot_revops.reports.generator import ReportGenerator
from hubspot_revops.schema.cache import get_or_discover_schema


def parse_time_range(period: str | None) -> TimeRange:
    """Parse a period string into a TimeRange."""
    now = datetime.now()
    if not period:
        return TimeRange(start=now - timedelta(days=90), end=now)

    period = period.lower().strip()
    if period.startswith("q"):
        # Quarter: Q1-2026, Q2-2026, etc.
        parts = period.split("-")
        quarter = int(parts[0][1])
        year = int(parts[1]) if len(parts) > 1 else now.year
        start_month = (quarter - 1) * 3 + 1
        start = datetime(year, start_month, 1)
        end_month = start_month + 2
        if end_month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end = datetime(year, end_month + 1, 1) - timedelta(days=1)
        return TimeRange(start=start, end=end)

    if period.endswith("d"):
        days = int(period[:-1])
        return TimeRange(start=now - timedelta(days=days), end=now)

    if period.endswith("m"):
        months = int(period[:-1])
        return TimeRange(start=now - timedelta(days=months * 30), end=now)

    return TimeRange(start=now - timedelta(days=90), end=now)


def main() -> None:
    parser = argparse.ArgumentParser(description="HubSpot RevOps Analyst")
    subparsers = parser.add_subparsers(dest="command")

    # Schema command
    schema_parser = subparsers.add_parser("schema", help="Discover and display CRM schema")
    schema_parser.add_argument("--refresh", action="store_true", help="Force schema refresh")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate a report")
    report_parser.add_argument(
        "type",
        choices=[
            "pipeline",
            "revenue",
            "funnel",
            "team",
            "activity",
            "executive",
            "closedlost",
            "forecast",
            "meetings",
        ],
        help="Report type",
    )
    report_parser.add_argument("--period", help="Time period (e.g., 90d, 6m, Q1-2026)")
    report_parser.add_argument(
        "--pipeline",
        help="Pipeline label or ID (case-insensitive). Use 'all' or omit for every pipeline.",
    )

    # Ask command
    ask_parser = subparsers.add_parser("ask", help="Ask a natural language question")
    ask_parser.add_argument("question", help="Your question about business metrics")
    ask_parser.add_argument("--period", help="Time period (e.g., 90d, 6m, Q1-2026)")
    ask_parser.add_argument(
        "--pipeline",
        help="Pipeline label or ID to scope the answer to (case-insensitive).",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    client = HubSpotClient()

    if args.command == "schema":
        schema = get_or_discover_schema(client, force_refresh=args.refresh)
        print(schema.summary())
        return

    schema = get_or_discover_schema(client)
    generator = ReportGenerator(client, schema)

    if args.command == "report":
        tr = parse_time_range(getattr(args, "period", None))
        pipeline_arg = getattr(args, "pipeline", None)
        reports = {
            "pipeline": generator.pipeline_report,
            "revenue": generator.revenue_report,
            "funnel": generator.funnel_report,
            "team": generator.rep_scorecard_report,
            "activity": generator.activity_report,
            "executive": generator.executive_summary,
            "closedlost": generator.closed_lost_report,
            "forecast": generator.forecast_report,
            "meetings": generator.meetings_report,
        }
        print(reports[args.type](time_range=tr, pipeline_id=pipeline_arg))

    elif args.command == "ask":
        tr = parse_time_range(getattr(args, "period", None))
        pipeline_arg = getattr(args, "pipeline", None)
        print(
            answer_question(
                args.question, generator, time_range=tr, pipeline_id=pipeline_arg
            )
        )


if __name__ == "__main__":
    main()
