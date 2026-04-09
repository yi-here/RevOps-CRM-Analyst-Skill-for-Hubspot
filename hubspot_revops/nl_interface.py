"""Natural language question → metric/report routing.

This module maps natural language questions to the appropriate metrics
and report generators. It uses keyword matching as a lightweight approach
that works well when the LLM has already done intent parsing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.reports.generator import ReportGenerator


@dataclass
class QueryIntent:
    """Parsed intent from a natural language question."""

    report_type: str  # pipeline, revenue, funnel, team, activity, forecast, executive
    metric: str | None = None  # Specific metric within the report
    time_range: TimeRange | None = None
    filters: dict | None = None  # e.g., {"owner": "John", "pipeline": "Sales"}


# Keyword → report type mapping
REPORT_KEYWORDS = {
    "pipeline": ["pipeline", "open deals", "stage", "stages", "funnel pipeline", "deal flow"],
    "revenue": ["revenue", "closed won", "mrr", "arr", "recurring", "bookings", "closed revenue"],
    "funnel": ["funnel", "conversion", "mql", "sql", "lifecycle", "lead source", "leads"],
    "team": ["rep", "reps", "team", "scorecard", "quota", "performance", "top performer", "rep scorecard"],
    "activity": ["activity", "activities", "calls", "emails", "meetings", "engagement", "touches"],
    "forecast": ["forecast", "weighted", "commit", "best case", "prediction"],
    "executive": ["summary", "overview", "executive", "dashboard", "health", "how are we doing"],
}

METRIC_KEYWORDS = {
    "win_rate": ["win rate", "close rate", "conversion rate deals"],
    "deal_size": ["deal size", "average deal", "avg deal"],
    "cycle_length": ["sales cycle", "cycle length", "time to close", "how long"],
    "velocity": ["velocity", "speed", "throughput"],
    "pipeline_value": ["pipeline value", "total pipeline", "open pipeline"],
    "churn": ["churn", "lost", "churned"],
    "nrr": ["net revenue retention", "nrr", "net retention"],
}


def classify_question(question: str) -> QueryIntent:
    """Classify a natural language question into a QueryIntent."""
    q = question.lower().strip()

    # Determine report type
    report_type = "executive"  # default
    max_score = 0
    for rtype, keywords in REPORT_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in q)
        if score > max_score:
            max_score = score
            report_type = rtype

    # Determine specific metric
    metric = None
    for m, keywords in METRIC_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            metric = m
            break

    return QueryIntent(report_type=report_type, metric=metric)


def answer_question(question: str, generator: ReportGenerator, time_range: TimeRange | None = None) -> str:
    """Route a natural language question to the appropriate report."""
    intent = classify_question(question)

    report_methods = {
        "pipeline": generator.pipeline_report,
        "revenue": generator.revenue_report,
        "funnel": generator.funnel_report,
        "team": generator.rep_scorecard_report,
        "activity": generator.activity_report,
        "forecast": generator.pipeline_report,  # Forecast is part of pipeline for now
        "executive": generator.executive_summary,
    }

    method = report_methods.get(intent.report_type, generator.executive_summary)
    return method(time_range=time_range)
