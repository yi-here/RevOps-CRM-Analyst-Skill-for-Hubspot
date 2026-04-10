"""Regression tests for the second bug-fix pass.

Each test references the bug number from the task description so a
future grep lands on the original failure mode.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from hubspot_revops.cli import parse_time_range
from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.owners import get_owners
from hubspot_revops.metrics import (
    closed_lost,
    conversion,
    forecast_bucket,
    meeting_history,
    pipeline,
    revenue,
    team,
)
from hubspot_revops.metrics._utils import parse_hs_datetime, pick_primary_currency
from hubspot_revops.reports.templates import (
    format_closed_lost_report,
    format_executive_summary,
    format_funnel_report,
    format_pipeline_report,
)
from hubspot_revops.schema.models import Owner


def _tr():
    return TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())


def _owners():
    return {"A": Owner(owner_id="A", first_name="Alice", last_name="Smith")}


# --- Bug 1: funnel report graceful fallback on contacts 502 -----------------


def test_funnel_returns_error_payload_on_contacts_failure():
    """Contacts search raising must not crash funnel_conversion_rates."""
    extractor = MagicMock()
    extractor.get_new_contacts.side_effect = Exception("502 Bad Gateway")

    result = conversion.funnel_conversion_rates(extractor, _tr())
    assert result["total_contacts"] == 0
    assert result["error"] == "502 Bad Gateway"


def test_lead_source_breakdown_survives_contacts_failure():
    extractor = MagicMock()
    extractor.get_new_contacts.side_effect = Exception("502 Bad Gateway")

    result = conversion.lead_source_breakdown(extractor, _tr())
    assert isinstance(result, pd.DataFrame)
    assert result.empty


# --- Bug 2: meeting_history NaN subtraction ---------------------------------


def test_meeting_history_handles_deals_without_meetings():
    """A deal with zero meetings must not crash the time-to-close step."""
    deal_extractor = MagicMock()
    deal_extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "dealname": ["Alpha", "Beta"],
        "hubspot_owner_id": ["A", "A"],
        "hs_is_closed_won": ["true", "false"],
        "amount": ["10000", "5000"],
        "pipeline": ["default", "default"],
        "closedate": ["2026-04-05", "2026-04-06"],
    })
    # Zero meetings for every deal — association lookup returns empty lists.
    deal_extractor.get_associated_ids.return_value = {"1": [], "2": []}

    activity_extractor = MagicMock()
    activity_extractor.get_activities.return_value = pd.DataFrame()

    data = meeting_history.meeting_history(
        deal_extractor, activity_extractor, _tr(), owners=_owners()
    )
    # Nothing blew up, and the median is 0 (all NaN rows).
    assert data["closed_deals_analyzed"] == 2
    assert data["total_meetings"] == 0
    assert data["time_to_close"]["median_days_won"] == 0.0
    assert data["time_to_close"]["median_days_lost"] == 0.0


def test_meeting_history_mixes_with_and_without_meetings():
    """A deal with meetings should compute time-to-close while a deal
    without meetings stays NaT and is excluded from the median."""
    deal_extractor = MagicMock()
    deal_extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "dealname": ["Alpha", "Beta"],
        "hubspot_owner_id": ["A", "A"],
        "hs_is_closed_won": ["true", "true"],
        "amount": ["10000", "5000"],
        "pipeline": ["default", "default"],
        "closedate": ["2026-04-10T00:00:00Z", "2026-04-10T00:00:00Z"],
    })
    deal_extractor.get_associated_ids.return_value = {"1": ["m1"], "2": []}

    activity_extractor = MagicMock()
    activity_extractor.get_activities.return_value = pd.DataFrame({
        "id": ["m1"],
        "hs_meeting_start_time": ["2026-04-01T00:00:00Z"],
    })

    data = meeting_history.meeting_history(
        deal_extractor, activity_extractor, _tr(), owners=_owners()
    )
    # Exactly 9 days for deal 1, NaT dropped for deal 2.
    assert data["time_to_close"]["median_days_won"] == pytest.approx(9.0, abs=0.01)


# --- Bug 3: owner pagination ------------------------------------------------


def test_get_owners_walks_all_pages():
    client = MagicMock()
    # First page: 2 owners + a next-page cursor. Second page: 1 owner, no cursor.
    page_one = SimpleNamespace(
        results=[
            SimpleNamespace(id="1", email="a@x", first_name="A", last_name="One"),
            SimpleNamespace(id="2", email="b@x", first_name="B", last_name="Two"),
        ],
        paging=SimpleNamespace(next=SimpleNamespace(after="cursor-2")),
    )
    page_two = SimpleNamespace(
        results=[
            SimpleNamespace(id="3", email="c@x", first_name="C", last_name="Three"),
        ],
        paging=None,
    )
    client.get_owners.side_effect = [page_one, page_two]

    owners = get_owners(client)
    assert set(owners.keys()) == {"1", "2", "3"}
    # Verify we asked for the cursor on the second call.
    assert client.get_owners.call_count == 2
    assert client.get_owners.call_args_list[1].kwargs["after"] == "cursor-2"


# --- Bug 4 + 6: stage probability parsing -----------------------------------


def test_normalize_probability_handles_noisy_floats():
    # Simulates HubSpot's "0.80000000000000004" drift.
    assert forecast_bucket._normalize_probability(0.80000000000000004) == 0.8
    # Percentage form.
    assert forecast_bucket._normalize_probability(80) == 0.8
    # String form.
    assert forecast_bucket._normalize_probability("0.8000000000001") == 0.8
    # Garbage falls back to zero instead of raising.
    assert forecast_bucket._normalize_probability(None) == 0.0
    assert forecast_bucket._normalize_probability("xyz") == 0.0


def test_forecast_bucket_commit_survives_float_precision():
    """A stage whose raw probability is 0.80000000000000004 must land in
    Commit — the previous comparison silently bucketed this as Best Case."""
    df = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["A"],
        "amount": ["10000"],
        "dealstage": ["negotiation"],
        "deal_currency_code": ["USD"],
        "pipeline": ["default"],
        "hs_forecast_category": [""],
        "hs_next_step": [""],
    })
    extractor = MagicMock()
    extractor.search_in_time_range.return_value = df

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {
                    "stage_id": "negotiation",
                    "label": "Negotiation",
                    "probability": 0.80000000000000004,
                })(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    assert data["per_rep"].iloc[0]["bucket"] == "Commit"


def test_forecast_bucket_proposal_label_is_commit():
    df = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["A"],
        "amount": ["10000"],
        "dealstage": ["proposal_stage"],
        "deal_currency_code": ["USD"],
        "pipeline": ["default"],
        "hs_forecast_category": [""],
        "hs_next_step": [""],
    })
    extractor = MagicMock()
    extractor.search_in_time_range.return_value = df

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {
                    "stage_id": "proposal_stage",
                    # Under 0.8, so the rule relies on the label match.
                    "label": "Proposal Sent",
                    "probability": 0.6,
                })(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    assert data["per_rep"].iloc[0]["bucket"] == "Commit"


# --- Bug 7: currency grouping in revenue ------------------------------------


def test_closed_revenue_groups_by_currency():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["1000000", "500", "800"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
        "hs_is_closed_won": ["true", "true", "true"],
    })

    result = revenue.closed_revenue(extractor, _tr())
    by_currency = result["by_currency"]
    assert set(by_currency.keys()) == {"JPY", "USD"}
    assert by_currency["JPY"]["total_revenue"] == 1_000_000
    assert by_currency["USD"]["total_revenue"] == 1300
    # Primary currency = USD (more deals).
    assert result["primary_currency"] == "USD"
    # Back-compat: top-level total_revenue reflects primary (USD), never
    # a JPY + USD mix.
    assert result["total_revenue"] == 1300


def test_revenue_by_owner_splits_currencies():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["1000000", "500", "800"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
        "hubspot_owner_id": ["A", "A", "A"],
        "hs_is_closed_won": ["true", "true", "true"],
    })
    owners = {"A": Owner(owner_id="A", first_name="Alice", last_name="Smith")}

    df = revenue.revenue_by_owner(extractor, _tr(), owners)
    assert set(df["currency"]) == {"JPY", "USD"}
    jpy = df[df["currency"] == "JPY"].iloc[0]
    usd = df[df["currency"] == "USD"].iloc[0]
    assert jpy["total_revenue"] == 1_000_000
    assert usd["total_revenue"] == 1300


# --- Bug 8: calendar-month period -------------------------------------------


def test_parse_time_range_month_keyword_is_calendar_month():
    now = datetime(2026, 4, 11, 14, 30)
    tr = parse_time_range("month", now=now)
    # Start must be April 1, not March 12.
    assert tr.start == datetime(2026, 4, 1)
    assert tr.end == now


def test_parse_time_range_this_month_alias():
    now = datetime(2026, 4, 11, 14, 30)
    tr = parse_time_range("this-month", now=now)
    assert tr.start == datetime(2026, 4, 1)
    assert tr.end == now


def test_parse_time_range_last_month():
    now = datetime(2026, 4, 11)
    tr = parse_time_range("last-month", now=now)
    assert tr.start == datetime(2026, 3, 1)
    # End is capped at month-end; for a prior month that's March 31.
    assert tr.end.year == 2026
    assert tr.end.month == 3
    assert tr.end.day == 31


def test_parse_time_range_month_name():
    now = datetime(2026, 4, 11)
    tr = parse_time_range("january", now=now)
    assert tr.start == datetime(2026, 1, 1)
    assert tr.end.month == 1
    assert tr.end.day == 31


def test_parse_time_range_month_name_abbrev():
    now = datetime(2026, 4, 11)
    tr = parse_time_range("apr", now=now)
    assert tr.start == datetime(2026, 4, 1)
    # April isn't finished yet; end is capped at now rather than 4/30.
    assert tr.end == now


# --- Fix A: Pipeline report header clarifies snapshot vs period -------------


def test_pipeline_report_header_shows_snapshot_not_period():
    """The open-pipeline section must not claim to represent a period
    — HubSpot does not expose historical pipeline state, so labeling
    today's open deals as "Q1-2026 pipeline" misled readers."""
    data = {
        "total": {"total_deals": 3, "total_value": 60_000.0, "avg_deal_size": 20_000.0},
        "by_stage": pd.DataFrame(),
        "win_rate": {"win_rate": 60.0, "won": 3, "lost": 2},
        "velocity": {"velocity_per_month": 100_000, "avg_cycle_days": 30},
        "cycle": {"avg_days": 30, "median_days": 28},
    }
    tr = TimeRange(start=datetime(2026, 1, 1), end=datetime(2026, 3, 31))
    out = format_pipeline_report(data, tr)

    # The header must flag the snapshot nature; the period label only
    # appears next to time-bound metrics.
    assert "Open pipeline snapshot" in out
    assert "does not affect open-deal totals" in out
    assert "Period Metrics" in out
    # Old label — if this reappears the fix regressed.
    assert "**Period:** 2026-01-01" not in out


# --- Fix B: Closed-lost currency grouping -----------------------------------


def test_closed_lost_separates_jpy_and_usd():
    """A ¥990K Japan-pipeline loss must not inflate USD lost totals."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "hubspot_owner_id": ["A", "A", "B"],
        "hs_is_closed_won": ["false", "false", "false"],
        "amount": ["990000", "500", "800"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
        "closed_lost_reason": ["Price", "Price", "Competitor"],
    })
    extractor.get_associated_ids.return_value = {"1": ["m"], "2": ["m"], "3": ["m"]}

    owners = {
        "A": Owner(owner_id="A", first_name="Alice", last_name="Smith"),
        "B": Owner(owner_id="B", first_name="Bob", last_name="Jones"),
    }
    data = closed_lost.closed_lost_analysis(extractor, _tr(), owners)

    by_currency = data["by_currency"]
    assert set(by_currency.keys()) == {"JPY", "USD"}
    assert by_currency["JPY"]["total_lost_value"] == 990_000
    assert by_currency["JPY"]["total_lost_deals"] == 1
    assert by_currency["USD"]["total_lost_value"] == 1300
    assert by_currency["USD"]["total_lost_deals"] == 2
    # Total deal count is currency-agnostic.
    assert data["total_lost_deals"] == 3
    # Top-level back-compat value must reflect *primary* currency only
    # (USD here — more deals), never JPY + USD summed.
    assert data["primary_currency"] == "USD"
    assert data["total_lost_value"] == 1300


def test_closed_lost_template_renders_per_currency_sections():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "hubspot_owner_id": ["A", "B"],
        "hs_is_closed_won": ["false", "false"],
        "amount": ["990000", "500"],
        "deal_currency_code": ["JPY", "USD"],
        "pipeline": ["japan", "default"],
        "closed_lost_reason": ["Price", "Competitor"],
    })
    extractor.get_associated_ids.return_value = {"1": ["m"], "2": ["m"]}
    owners = {
        "A": Owner(owner_id="A", first_name="Alice", last_name="Smith"),
        "B": Owner(owner_id="B", first_name="Bob", last_name="Jones"),
    }
    data = closed_lost.closed_lost_analysis(extractor, _tr(), owners)
    out = format_closed_lost_report(data, _tr())

    # Both currency sections must appear, neither summed.
    assert "## JPY" in out
    assert "## USD" in out
    # Yen symbol rendered, no "$990" USD bleed.
    assert "¥990.0K" in out or "¥990,000" in out
    assert "Multi-currency losses are reported separately" in out


def test_closed_lost_single_currency_keeps_back_compat_shape():
    """Single-currency portals must render with the old section headers
    (no per-currency prefix) to minimise noise."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "hubspot_owner_id": ["A", "B"],
        "hs_is_closed_won": ["false", "false"],
        "amount": ["1000", "2000"],
        "deal_currency_code": ["USD", "USD"],
        "pipeline": ["default", "default"],
        "closed_lost_reason": ["Price", "Competitor"],
    })
    extractor.get_associated_ids.return_value = {"1": ["m"], "2": ["m"]}
    owners = {
        "A": Owner(owner_id="A", first_name="Alice", last_name="Smith"),
        "B": Owner(owner_id="B", first_name="Bob", last_name="Jones"),
    }
    data = closed_lost.closed_lost_analysis(extractor, _tr(), owners)
    out = format_closed_lost_report(data, _tr())

    assert "## Lost deals by rep" in out
    assert "## Reasons" in out
    # No currency prefix sections when only one currency exists.
    assert "## USD Lost deals" not in out


# --- Fix C: Quarter-end boundary includes full last day ---------------------


def test_parse_time_range_q1_ends_last_microsecond_of_march_31():
    tr = parse_time_range("Q1-2026")
    assert tr.start == datetime(2026, 1, 1)
    # End is the very last instant of March 31, not midnight (which is
    # really the start of March 31 and excluded all same-day deals).
    assert tr.end.year == 2026
    assert tr.end.month == 3
    assert tr.end.day == 31
    assert tr.end.hour == 23
    assert tr.end.minute == 59


def test_parse_time_range_q4_ends_last_microsecond_of_dec_31():
    tr = parse_time_range("Q4-2026")
    assert tr.start == datetime(2026, 10, 1)
    assert tr.end.year == 2026
    assert tr.end.month == 12
    assert tr.end.day == 31
    assert tr.end.hour == 23


# --- Team ↔ revenue consistency ---------------------------------------------


# --- Pipeline total currency mixing (third verification pass) --------------


def _pipeline_schema():
    """Minimal CRMSchema-ish object for pipeline_by_stage tests."""
    class FakeStage:
        def __init__(self, stage_id, label):
            self.stage_id = stage_id
            self.label = label
            self.probability = 0.5

    class FakePipeline:
        def __init__(self, pipeline_id, label, stages):
            self.pipeline_id = pipeline_id
            self.label = label
            self.stages = stages

    class FakeSchema:
        pipelines = {
            "deals": [
                FakePipeline("default", "Sales", [FakeStage("qualified", "Qualified")]),
                FakePipeline("japan", "Japan", [FakeStage("jp_qualified", "Qualified")]),
            ]
        }

    return FakeSchema()


def test_total_pipeline_value_groups_by_currency():
    """The Pipeline Report's $129.06M = ¥118M + $11M bug: open pipeline
    totals must never sum across currencies. This exercises the open
    side directly (the revenue bug-fix tests cover the closed side)."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["118000000", "5000000", "6000000"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
        "dealstage": ["jp_qualified", "qualified", "qualified"],
    })

    result = pipeline.total_pipeline_value(extractor)
    by_currency = result["by_currency"]
    assert set(by_currency.keys()) == {"JPY", "USD"}
    assert by_currency["JPY"]["total_value"] == 118_000_000
    assert by_currency["USD"]["total_value"] == 11_000_000
    # Primary is USD (2 deals vs 1); back-compat top-level reflects USD.
    assert result["primary_currency"] == "USD"
    assert result["total_value"] == 11_000_000
    # Deal count is universal — 3 open deals total.
    assert result["total_deals"] == 3


def test_pipeline_by_stage_splits_currencies_within_same_stage():
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "amount": ["1000000", "50000"],
        "deal_currency_code": ["JPY", "USD"],
        "pipeline": ["japan", "default"],
        "dealstage": ["jp_qualified", "qualified"],
    })
    df = pipeline.pipeline_by_stage(extractor, _pipeline_schema())
    assert "currency" in df.columns
    assert set(df["currency"]) == {"JPY", "USD"}
    jpy = df[df["currency"] == "JPY"].iloc[0]
    usd = df[df["currency"] == "USD"].iloc[0]
    assert jpy["total_value"] == 1_000_000
    assert usd["total_value"] == 50_000


def test_avg_deal_size_groups_by_currency():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["1000000", "50000", "30000"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
        "hs_is_closed_won": ["true", "true", "true"],
    })
    result = pipeline.avg_deal_size(extractor, _tr())
    by_currency = result["by_currency"]
    assert by_currency["JPY"]["avg_deal_size"] == 1_000_000
    assert by_currency["USD"]["avg_deal_size"] == 40_000
    assert result["primary_currency"] == "USD"
    # Top-level avg_deal_size is PRIMARY ONLY, not a cross-currency avg.
    assert result["avg_deal_size"] == 40_000


def test_pipeline_velocity_uses_primary_currency_only():
    """Velocity must be denominated in a single currency — mixing a
    JPY avg_size with a USD open-deal count produces a nonsense number."""
    extractor = MagicMock()
    # 2 USD open deals, 1 JPY open.
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["100000", "200000", "5000000"],
        "deal_currency_code": ["USD", "USD", "JPY"],
        "pipeline": ["default", "default", "japan"],
        "dealstage": ["qualified", "qualified", "jp_qualified"],
    })
    # Won deals: 1 USD @ $50K avg, 1 JPY @ ¥1M avg.
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["w1", "w2"],
        "amount": ["50000", "1000000"],
        "deal_currency_code": ["USD", "JPY"],
        "pipeline": ["default", "japan"],
        "hs_is_closed_won": ["true", "true"],
        "closedate": ["2026-03-15T00:00:00Z", "2026-03-15T00:00:00Z"],
        "createdate": ["2026-01-15T00:00:00Z", "2026-01-15T00:00:00Z"],
    })

    vel = pipeline.pipeline_velocity(extractor, _tr())
    # Primary currency is USD (2 open deals vs 1 JPY).
    assert vel["currency"] == "USD"
    # open_deals in velocity must be the USD-only count, not the total.
    assert vel["open_deals"] == 2
    # avg_deal_size is pulled from the USD bucket of won deals.
    assert vel["avg_deal_size"] == 50_000


def test_pipeline_report_template_renders_per_currency_totals():
    total = pipeline.total_pipeline_value(MagicMock(
        **{"get_open_deals.return_value": pd.DataFrame({
            "id": ["1", "2", "3"],
            "amount": ["118000000", "5000000", "6000000"],
            "deal_currency_code": ["JPY", "USD", "USD"],
            "pipeline": ["japan", "default", "default"],
            "dealstage": ["jp_qualified", "qualified", "qualified"],
        })}
    ))
    data = {
        "total": total,
        "by_stage": pd.DataFrame(),
        "win_rate": {"win_rate": 50.0, "won": 1, "lost": 1},
        "velocity": {"velocity_per_month": 100_000, "avg_cycle_days": 30, "currency": "USD"},
        "cycle": {"avg_days": 30, "median_days": 28},
    }
    out = format_pipeline_report(data, _tr())
    # Both currency lines present, no mixed sum.
    assert "JPY" in out
    assert "USD" in out
    # The old "$129.06M" bug would show the sum; ensure it doesn't.
    assert "$129" not in out
    assert "Multi-currency totals are reported separately" in out


def test_executive_summary_template_renders_multi_currency_pipeline():
    """The exec summary's Open Pipeline line must not be a single
    mixed total. Each currency gets its own sub-line inside the cell."""
    data = {
        "pipeline": {
            "by_currency": {
                "JPY": {"deal_count": 1, "total_value": 118_000_000, "avg_deal_size": 118_000_000},
                "USD": {"deal_count": 2, "total_value": 11_000_000, "avg_deal_size": 5_500_000},
            },
            "primary_currency": "USD",
            "total_deals": 3,
            "total_value": 11_000_000,
            "avg_deal_size": 5_500_000,
        },
        "win_rate": {"win_rate": 50.0, "won": 1, "lost": 1},
        "avg_deal_size": {
            "by_currency": {
                "USD": {"deal_count": 1, "avg_deal_size": 50_000, "total_revenue": 50_000},
            },
            "primary_currency": "USD",
            "avg_deal_size": 50_000,
            "total_revenue": 50_000,
            "deal_count": 1,
        },
        "velocity": {"velocity_per_month": 100_000, "avg_cycle_days": 30, "currency": "USD"},
        "revenue": {
            "by_currency": {"USD": {"total_revenue": 50_000, "deal_count": 1}},
            "primary_currency": "USD",
            "total_revenue": 50_000,
            "deal_count": 1,
            "total_deals": 1,
        },
        "weighted": {"weighted_value": 60_000, "unweighted_value": 100_000, "deal_count": 3},
    }
    out = format_executive_summary(data, _tr())
    # Per-currency lines rendered inside the Open Pipeline cell.
    assert "JPY:" in out
    assert "USD:" in out
    assert "¥118.00M" in out
    # No silent $129M sum.
    assert "$129" not in out


# --- Funnel contact total truncation ----------------------------------------


def test_funnel_uses_server_reported_total_for_top_of_funnel():
    """A portal with 7.1M contacts must not display "10,000" as the
    top of the funnel. The Search API caps pagination at 10K but reports
    the true total via ``response.total``; we use that for display."""
    extractor = MagicMock()
    # The paginated fetch returns the 10K-row sample.
    sample = pd.DataFrame({
        "id": [str(i) for i in range(10_000)],
        "createdate": ["2026-03-01"] * 10_000,
        "hs_lifecyclestage_customer_date": [None] * 10_000,
    })
    sample.attrs["hs_total"] = 7_157_756
    extractor.get_new_contacts.return_value = sample
    extractor.count_via_search.return_value = 7_157_756

    result = conversion.funnel_conversion_rates(extractor, _tr())
    # The top-of-funnel reflects the server total, not the truncated sample.
    assert result["total_contacts"] == 7_157_756
    assert result["sampled_contacts"] == 10_000
    assert result["truncated"] is True
    # Subscriber stage count also matches the true total.
    assert result["stages"]["subscriber"] == 7_157_756


def test_funnel_template_renders_truncation_warning():
    data = {
        "funnel": {
            "stages": {"subscriber": 7_157_756, "lead": 100},
            "conversions": {},
            "total_contacts": 7_157_756,
            "sampled_contacts": 10_000,
            "truncated": True,
        },
        "sources": pd.DataFrame(),
    }
    out = format_funnel_report(data, _tr())
    assert "7,157,756" in out
    assert "10,000" in out
    assert "Sampled breakdown" in out


# --- Activity SDK do_search object_type argument ----------------------------


def test_search_objects_passes_object_type_positionally_for_generic_api():
    """Regression for "SearchApi.do_search() missing 1 required positional
    argument: 'object_type'". Engagement types (calls, emails, meetings,
    notes, tasks) fall back to the generic ``crm.objects.search_api``
    which requires ``object_type`` as a positional argument."""
    from hubspot_revops.client import HubSpotClient

    client = HubSpotClient.__new__(HubSpotClient)

    # Stand up a minimal fake SDK: top-level ``crm.objects.search_api``
    # exists but no dedicated ``crm.meetings`` module and no nested
    # ``crm.objects.meetings``. The call must go through the generic
    # path with ``object_type`` positional.
    calls = []

    class FakeSearchApi:
        def do_search(self, *args, **kwargs):
            calls.append({"args": args, "kwargs": kwargs})
            return SimpleNamespace(results=[], paging=None, total=0)

    class FakeObjectsApi:
        search_api = FakeSearchApi()

    class FakeCrm:
        objects = FakeObjectsApi()

    client.api = SimpleNamespace(crm=FakeCrm())

    class FakeLimiter:
        def wait_if_needed(self):
            pass

    client.rate_limiter = FakeLimiter()
    client.search_rate_limiter = FakeLimiter()

    def _rate_limited(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    client._rate_limited = _rate_limited

    client.search_objects(
        object_type="calls",
        filter_groups=[{"filters": []}],
    )

    assert len(calls) == 1
    # "calls" must have been passed as the first positional arg to the
    # generic objects search API.
    assert calls[0]["args"][0] == "calls"
    assert "public_object_search_request" in calls[0]["kwargs"]


# --- Meeting timestamp parsing (timezone / epoch-ms) ------------------------


def test_parse_hs_datetime_handles_epoch_ms_strings():
    """hs_meeting_start_time comes back as epoch-ms strings like
    "1712345678000". The old ``pd.to_datetime(..., utc=True)`` path
    parsed those as year 1712345678 and returned NaT, which is why
    'Median days first meeting → close' was stuck at 0.0."""
    # 2026-04-01 00:00:00 UTC as epoch ms.
    epoch_ms = str(int(datetime(2026, 4, 1).timestamp() * 1000))
    series = pd.Series([epoch_ms, "", None])
    parsed = parse_hs_datetime(series)
    assert parsed.iloc[0].year == 2026
    assert parsed.iloc[0].month == 4
    assert pd.isna(parsed.iloc[1])
    assert pd.isna(parsed.iloc[2])


def test_parse_hs_datetime_handles_iso_strings():
    series = pd.Series(["2026-04-01T12:30:00Z", "2026-03-15"])
    parsed = parse_hs_datetime(series)
    assert parsed.iloc[0].year == 2026
    assert parsed.iloc[0].month == 4
    assert parsed.iloc[1].year == 2026
    assert parsed.iloc[1].month == 3


def test_meeting_history_median_nonzero_with_epoch_ms_meetings():
    """End-to-end regression for the 0.0 median bug: feed epoch-ms
    hs_meeting_start_time values and assert the median days-to-close
    is the real value, not 0."""
    deal_extractor = MagicMock()
    deal_extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1"],
        "dealname": ["Alpha"],
        "hubspot_owner_id": ["A"],
        "hs_is_closed_won": ["true"],
        "amount": ["10000"],
        "pipeline": ["default"],
        # closedate as ISO for variety.
        "closedate": ["2026-04-15T00:00:00Z"],
    })
    deal_extractor.get_associated_ids.return_value = {"1": ["m1"]}

    activity_extractor = MagicMock()
    meeting_epoch_ms = str(int(datetime(2026, 4, 1).timestamp() * 1000))
    activity_extractor.get_activities.return_value = pd.DataFrame({
        "id": ["m1"],
        "hs_meeting_start_time": [meeting_epoch_ms],
    })

    owners = {"A": Owner(owner_id="A", first_name="Alice", last_name="Smith")}
    data = meeting_history.meeting_history(
        deal_extractor, activity_extractor, _tr(), owners=owners
    )
    # 14 days between April 1 and April 15.
    assert data["time_to_close"]["median_days_won"] == pytest.approx(14.0, abs=0.1)


# --- pick_primary_currency helper -------------------------------------------


def test_pick_primary_currency_picks_highest_count():
    assert (
        pick_primary_currency({
            "JPY": {"deal_count": 1},
            "USD": {"deal_count": 5},
        })
        == "USD"
    )


def test_pick_primary_currency_ties_break_alphabetically_later():
    """Matches the existing revenue/closed_lost behaviour: on a tie,
    lexicographically later code wins ("USD" > "JPY")."""
    assert (
        pick_primary_currency({
            "JPY": {"deal_count": 5},
            "USD": {"deal_count": 5},
        })
        == "USD"
    )


def test_pick_primary_currency_custom_count_key():
    assert (
        pick_primary_currency(
            {
                "JPY": {"total_lost_deals": 10},
                "USD": {"total_lost_deals": 3},
            },
            count_key="total_lost_deals",
        )
        == "JPY"
    )


def test_team_and_revenue_agree_on_won_totals_with_capitalized_boolean():
    """Regression for the Noah ~$25K discrepancy: HubSpot's SDK can
    return ``hs_is_closed_won`` as "True" (capitalized) or as Python
    True — either way the team report's Python filter accepted it, but
    revenue's ``won_only=True`` API filter (strict string EQ "true")
    did not. After routing revenue through the same fetch + filter
    path, the two must agree on every mixed-case variant."""
    rows = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "hubspot_owner_id": ["N", "N", "N", "N"],
        # Mix of lower, upper, title, and Python-bool-as-str forms.
        "hs_is_closed_won": ["true", "True", "TRUE", "True"],
        "amount": ["30000", "40000", "50000", "31300"],
        "deal_currency_code": ["USD", "USD", "USD", "USD"],
        "pipeline": ["default", "default", "default", "default"],
        "hs_is_closed": ["true", "true", "true", "true"],
    })

    extractor = MagicMock()
    extractor.get_closed_deals.return_value = rows
    extractor.get_open_deals.return_value = pd.DataFrame()

    owners = {"N": Owner(owner_id="N", first_name="Noah", last_name="Liam")}

    team_scorecard = team.rep_scorecard(extractor, _tr(), owners)
    # Reset mock side-effects; both metrics should see the same rows.
    extractor.get_closed_deals.return_value = rows
    rev = revenue.closed_revenue(extractor, _tr())
    extractor.get_closed_deals.return_value = rows
    rev_by_owner = revenue.revenue_by_owner(extractor, _tr(), owners)

    team_total = float(team_scorecard[team_scorecard["currency"] == "USD"]["closed_won_revenue"].iloc[0])
    rev_total = float(rev["by_currency"]["USD"]["total_revenue"])
    rev_by_owner_total = float(
        rev_by_owner[rev_by_owner["currency"] == "USD"]["total_revenue"].iloc[0]
    )

    assert team_total == rev_total == rev_by_owner_total == 151_300
