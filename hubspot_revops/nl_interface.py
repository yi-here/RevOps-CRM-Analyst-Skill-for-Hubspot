"""Natural language question → metric/report routing.

This module maps natural language questions to the appropriate metrics
and report generators. It uses keyword matching as a lightweight approach
that works well when the LLM has already done intent parsing — and
importantly, when it DOESN'T match any canned metric, it surfaces a
structured fallback message so the agent can hand off to HubSpot's
official MCP server for a raw-records lookup instead of silently
routing to the wrong report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.reports.generator import ReportGenerator

log = logging.getLogger(__name__)


@dataclass
class QueryIntent:
    """Parsed intent from a natural language question.

    ``confidence`` distinguishes "I found a matching canned metric" from
    "I guessed". When confidence is ``"low"``, callers should surface
    the MCP-fallback message instead of routing to the default report.
    """

    report_type: str | None  # None when no keyword matched
    metric: str | None = None  # Specific metric within the report
    time_range: TimeRange | None = None
    filters: dict | None = None  # e.g., {"owner": "John", "pipeline": "Sales"}
    confidence: str = "high"  # "high" when at least one keyword matched, else "low"
    matched_keywords: list[str] = field(default_factory=list)


# Keyword → report type mapping
REPORT_KEYWORDS = {
    "pipeline": ["pipeline", "open deals", "stage", "stages", "funnel pipeline", "deal flow"],
    "revenue": ["revenue", "closed won", "mrr", "arr", "recurring", "bookings", "closed revenue"],
    "funnel": ["funnel", "conversion", "mql", "sql", "lifecycle", "lead source", "leads"],
    "team": ["rep", "reps", "team", "scorecard", "quota", "performance", "top performer", "rep scorecard"],
    "activity": ["activity", "activities", "calls", "emails", "engagement", "touches"],
    "forecast": ["forecast", "weighted", "commit", "best case", "prediction"],
    "closed_lost": ["lost deals", "closed lost", "losses", "loss reason", "lost reason", "churn reason"],
    "meetings": ["meetings", "meeting history", "meeting count", "effort sink"],
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
    """Classify a natural language question into a QueryIntent.

    Scores every report type by keyword hits, picks the highest score,
    and breaks ties alphabetically for determinism. A question that
    hits no report keyword but DOES hit a metric keyword (e.g.
    "what's our win rate?") still gets ``confidence="high"`` and
    routes to the executive summary, since that report surfaces
    every top-level metric. A question that matches neither reports
    nor metrics returns ``confidence="low"`` and ``report_type=None``
    so callers can fall back to HubSpot's MCP instead of silently
    routing to the default executive summary.
    """
    q = question.lower().strip()

    scores: dict[str, int] = {}
    all_matched: list[str] = []
    for rtype, keywords in REPORT_KEYWORDS.items():
        hits = [kw for kw in keywords if kw in q]
        if hits:
            scores[rtype] = len(hits)
            all_matched.extend(hits)

    # Metric detection is independent of report routing — the caller
    # gets it as metadata regardless of which report is chosen.
    metric = None
    metric_matched: list[str] = []
    for m, keywords in METRIC_KEYWORDS.items():
        hits = [kw for kw in keywords if kw in q]
        if hits:
            metric = m
            metric_matched.extend(hits)
            break

    if scores:
        # Highest score wins; ties broken alphabetically (deterministic
        # and gives "calls did the team" the activity route instead of
        # team, which matches user expectation since "calls" is a
        # stronger activity signal than "team" is a team signal).
        top_score = max(scores.values())
        report_type = min(rt for rt, s in scores.items() if s == top_score)
        return QueryIntent(
            report_type=report_type,
            metric=metric,
            confidence="high",
            matched_keywords=all_matched + metric_matched,
        )

    if metric:
        # Metric matched but no specific report — the user clearly
        # asked for a known measure, we just don't have a dedicated
        # report for it. Route to the executive summary which surfaces
        # every top-level metric (win rate, velocity, avg deal size,
        # cycle length, total pipeline, closed revenue).
        return QueryIntent(
            report_type="executive",
            metric=metric,
            confidence="high",
            matched_keywords=metric_matched,
        )

    # Neither matched — genuinely no signal. Report_type stays None so
    # ``answer_question`` routes to the MCP fallback banner.
    return QueryIntent(report_type=None, confidence="low", matched_keywords=[])


def format_mcp_fallback(
    question: str,
    *,
    reason: str = "no_match",
    attempted_report: str | None = None,
    error: str | None = None,
) -> str:
    """Structured fallback banner directing the agent to HubSpot's MCP.

    Emitted instead of a report when the skill cannot confidently
    answer a question — either because no keyword matched
    (``reason="no_match"``) or because the chosen report method
    raised an exception (``reason="runtime_error"``). The banner is
    markdown so an LLM host renders it cleanly and a human reading
    the CLI output sees exactly what happened.

    The agent contract:
      1. If the output contains ``FALLBACK_TO_MCP``, the skill did
         NOT answer the question and the agent should call HubSpot's
         MCP tools (``search_deals``, ``search_contacts``,
         ``get_engagements``, etc.) using the original question to
         build the right filter set.
      2. The agent may also retry with a rephrased question that
         hits a canned metric if one obviously applies.
    """
    lines: list[str] = [
        "> ⚠️ **FALLBACK_TO_MCP** — the skill could not confidently answer this.",
        ">",
    ]
    if reason == "runtime_error" and error:
        lines.append(
            f"> The `{attempted_report}` report raised an error while running: `{error}`"
        )
        lines.append(
            "> This usually means a transient HubSpot API issue or a schema "
            "mismatch that a raw-records lookup can work around."
        )
    else:
        lines.append(
            "> This question didn't match any canned RevOps metric in the "
            "skill's keyword map, so the skill has no deterministic answer."
        )
    lines.extend([
        ">",
        "> **Agent — next steps:** fall back to HubSpot's official MCP server "
        "and answer from raw CRM records. Useful MCP tools:",
        ">",
        "> - `search_deals(filter_groups=...)` — filtered deal lookups",
        "> - `search_contacts(filter_groups=...)` — filtered contact lookups",
        "> - `search_companies(filter_groups=...)` — filtered company lookups",
        "> - `get_deal(id)` / `get_contact(id)` / `get_company(id)` — single record",
        "> - `get_engagements(...)` — calls, emails, meetings, notes, tasks",
        ">",
        f'> **Original question:** "{question}"',
        ">",
        "> If HubSpot's MCP is not installed, install it with:",
        "> ```",
        "> claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic",
        "> ```",
    ])
    return "\n".join(lines)


def answer_question(
    question: str,
    generator: ReportGenerator,
    time_range: TimeRange | None = None,
    pipeline_id: str | None = None,
) -> str:
    """Route a natural language question to the appropriate report.

    When the question cannot be classified (no keyword match) or the
    chosen report method crashes, returns a structured
    ``FALLBACK_TO_MCP`` banner directing the agent to answer from
    HubSpot's MCP server instead. This is better than silently
    routing to the executive summary (the old default), which
    produced a valid but irrelevant output for any question outside
    the canned metric catalogue.
    """
    intent = classify_question(question)

    if intent.confidence != "high" or intent.report_type is None:
        return format_mcp_fallback(question, reason="no_match")

    report_methods = {
        "pipeline": generator.pipeline_report,
        "revenue": generator.revenue_report,
        "funnel": generator.funnel_report,
        "team": generator.rep_scorecard_report,
        "activity": generator.activity_report,
        "forecast": generator.forecast_report,
        "closed_lost": generator.closed_lost_report,
        "meetings": generator.meetings_report,
        "executive": generator.executive_summary,
    }

    method = report_methods.get(intent.report_type)
    if method is None:
        return format_mcp_fallback(question, reason="no_match")

    try:
        return method(time_range=time_range, pipeline_id=pipeline_id)
    except Exception as exc:
        log.warning(
            "answer_question: %s report raised, falling back to MCP: %s",
            intent.report_type,
            exc,
        )
        return format_mcp_fallback(
            question,
            reason="runtime_error",
            attempted_report=intent.report_type,
            error=str(exc),
        )
