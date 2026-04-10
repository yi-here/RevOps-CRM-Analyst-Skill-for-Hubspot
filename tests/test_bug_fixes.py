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
from hubspot_revops.metrics import conversion, forecast_bucket, meeting_history, revenue
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
