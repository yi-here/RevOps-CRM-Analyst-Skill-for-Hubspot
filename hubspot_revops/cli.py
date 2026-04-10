"""CLI entry point for the HubSpot RevOps Analyst Skill."""

from __future__ import annotations

import argparse
import calendar
import sys
from datetime import datetime, timedelta

from hubspot_revops.client import HubSpotClient
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.nl_interface import answer_question
from hubspot_revops.reports.generator import ReportGenerator
from hubspot_revops.schema.cache import get_or_discover_schema


MONTH_NAMES = {name.lower(): idx for idx, name in enumerate(calendar.month_name) if name}
MONTH_NAMES.update({name.lower(): idx for idx, name in enumerate(calendar.month_abbr) if name})


def _calendar_month(year: int, month: int, *, now: datetime) -> TimeRange:
    """Full-calendar-month TimeRange, capped at ``now`` for the current month."""
    start = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = datetime(year, month, last_day, 23, 59, 59)
    # Don't advertise future dates as "closed" data.
    if end > now:
        end = now
    return TimeRange(start=start, end=end)


def parse_time_range(period: str | None, *, now: datetime | None = None) -> TimeRange:
    """Parse a period string into a TimeRange.

    Supported formats:
    - ``90d``, ``30d``       — last N days rolling
    - ``6m``                 — last N × 30 days rolling
    - ``Q1-2026``            — calendar quarter
    - ``month`` / ``this-month`` / ``thismonth`` — current calendar month
      (1st of this month through today). Using ``30d`` for a monthly
      report misleads users because it never lines up with the month
      boundary — e.g. on April 11 it returned March 12 – April 11.
    - ``last-month``         — previous full calendar month
    - ``april``, ``jan``, …  — full calendar month in the current year
    """
    now = now or datetime.now()
    if not period:
        return TimeRange(start=now - timedelta(days=90), end=now)

    period = period.lower().strip().replace("_", "-")

    if period in {"month", "this-month", "thismonth", "mtd", "month-to-date"}:
        return TimeRange(start=datetime(now.year, now.month, 1), end=now)

    if period in {"last-month", "lastmonth", "prev-month", "previous-month"}:
        prev_month = now.month - 1 or 12
        prev_year = now.year if now.month > 1 else now.year - 1
        return _calendar_month(prev_year, prev_month, now=now)

    if period in MONTH_NAMES:
        return _calendar_month(now.year, MONTH_NAMES[period], now=now)

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
    report_parser.add_argument(
        "--period",
        help=(
            "Time period. Supports 90d / 6m rolling, Q1-2026 quarters, "
            "month / this-month (current calendar month-to-date), "
            "last-month, or a month name like april."
        ),
    )
    report_parser.add_argument(
        "--pipeline",
        help="Pipeline label or ID (case-insensitive). Use 'all' or omit for every pipeline.",
    )

    # Ask command
    ask_parser = subparsers.add_parser("ask", help="Ask a natural language question")
    ask_parser.add_argument("question", help="Your question about business metrics")
    ask_parser.add_argument(
        "--period",
        help=(
            "Time period. Supports 90d / 6m rolling, Q1-2026 quarters, "
            "month / this-month (current calendar month-to-date), "
            "last-month, or a month name like april."
        ),
    )
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
