"""Tests for the natural language question classifier."""

from unittest.mock import MagicMock

from hubspot_revops.nl_interface import (
    answer_question,
    classify_question,
    format_mcp_fallback,
)


def test_pipeline_questions():
    assert classify_question("What's our pipeline looking like?").report_type == "pipeline"
    assert classify_question("Show me open deals by stage").report_type == "pipeline"


def test_revenue_questions():
    assert classify_question("How much revenue did we close this quarter?").report_type == "revenue"
    assert classify_question("What's our ARR?").report_type == "revenue"


def test_funnel_questions():
    assert classify_question("Show me the funnel conversion rates").report_type == "funnel"
    assert classify_question("How many MQLs converted to SQLs?").report_type == "funnel"
    assert classify_question("What are our lead sources?").report_type == "funnel"


def test_team_questions():
    assert classify_question("Who are the top performing reps?").report_type == "team"
    assert classify_question("Show me the rep scorecard").report_type == "team"


def test_activity_questions():
    assert classify_question("How many calls did the team make?").report_type == "activity"
    assert classify_question("Show me email engagement").report_type == "activity"


def test_forecast_questions():
    assert classify_question("What's the weighted forecast?").report_type == "forecast"


def test_executive_default():
    assert classify_question("How are we doing?").report_type == "executive"
    assert classify_question("Give me a summary").report_type == "executive"


def test_metric_detection():
    intent = classify_question("What's our win rate this quarter?")
    assert intent.metric == "win_rate"

    intent = classify_question("What's the average deal size?")
    assert intent.metric == "deal_size"

    intent = classify_question("How long is our sales cycle?")
    assert intent.metric == "cycle_length"


# ---------------------------------------------------------------------------
# MCP fallback protocol — when the skill can't confidently answer, emit a
# structured banner the agent can detect and hand off to HubSpot's MCP.
# ---------------------------------------------------------------------------


def test_classify_unknown_question_is_low_confidence():
    """No keyword match → low confidence, report_type is None so callers
    can branch on it explicitly instead of sentinel-string-matching."""
    intent = classify_question("What's the color of our website?")
    assert intent.confidence == "low"
    assert intent.report_type is None
    assert intent.matched_keywords == []


def test_classify_known_question_is_high_confidence():
    intent = classify_question("What's our pipeline?")
    assert intent.confidence == "high"
    assert intent.report_type == "pipeline"
    assert "pipeline" in intent.matched_keywords


def test_classify_tiebreak_is_alphabetical():
    """Ties break alphabetically — 'calls did the team' hits both
    'calls' (activity) and 'team' (team), and activity < team so
    activity wins. Matches user expectation that 'calls' is a stronger
    activity signal than 'team' is a team signal."""
    assert (
        classify_question("How many calls did the team make?").report_type
        == "activity"
    )


def test_format_mcp_fallback_contains_sentinel():
    """The banner MUST contain the FALLBACK_TO_MCP sentinel so the
    agent (or a grep) can detect fallback output unambiguously."""
    banner = format_mcp_fallback("What's the color of our website?")
    assert "FALLBACK_TO_MCP" in banner
    assert "What's the color of our website?" in banner
    # Points the agent at the right MCP tools.
    assert "search_deals" in banner
    assert "mcp.hubspot.com/anthropic" in banner


def test_format_mcp_fallback_includes_error_on_runtime_failure():
    banner = format_mcp_fallback(
        "Who won the most deals?",
        reason="runtime_error",
        attempted_report="team",
        error="502 Bad Gateway",
    )
    assert "FALLBACK_TO_MCP" in banner
    assert "502 Bad Gateway" in banner
    assert "team" in banner
    assert "Who won the most deals?" in banner


def test_answer_question_falls_back_when_no_keyword_matches():
    """Unknown questions route to the MCP fallback banner instead of
    silently defaulting to the executive summary."""
    generator = MagicMock()

    result = answer_question("What's the color of our website?", generator)

    assert "FALLBACK_TO_MCP" in result
    # Must NOT have called any report method — we didn't know which to pick.
    generator.executive_summary.assert_not_called()
    generator.pipeline_report.assert_not_called()
    generator.revenue_report.assert_not_called()


def test_answer_question_falls_back_when_report_raises():
    """Report-method exceptions surface as the fallback banner so the
    agent can hand off to MCP instead of showing the user a traceback."""
    generator = MagicMock()
    generator.pipeline_report.side_effect = Exception("HubSpot 502 Bad Gateway")

    result = answer_question("What's our pipeline?", generator)

    assert "FALLBACK_TO_MCP" in result
    assert "HubSpot 502 Bad Gateway" in result
    assert "pipeline" in result
    generator.pipeline_report.assert_called_once()


def test_answer_question_high_confidence_routes_to_report():
    """Happy path: a classifiable question routes to the right report
    method and returns its output unchanged (no fallback banner)."""
    generator = MagicMock()
    generator.revenue_report.return_value = "# Revenue Report\n..."

    result = answer_question("What's our closed revenue this quarter?", generator)

    assert result == "# Revenue Report\n..."
    assert "FALLBACK_TO_MCP" not in result
    generator.revenue_report.assert_called_once()
