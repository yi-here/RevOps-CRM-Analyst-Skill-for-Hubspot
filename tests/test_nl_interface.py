"""Tests for the natural language question classifier."""

from hubspot_revops.nl_interface import classify_question


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
