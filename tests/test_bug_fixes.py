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
from hubspot_revops.reports.templates import (
    format_closed_lost_report,
    format_pipeline_report,
)
from hubspot_revops.schema.models import Owner


def _tr():
    return TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())


def _owners():
    return {"A": Owner(owner_id="A", first_name="Alice", last_name="Smith")}


# --- Bug 1: funnel report graceful fallback on contacts 502 -----------------


def test_funnel_returns_error_payload_on_contacts_failure():
    """Contacts count raising must not crash funnel_conversion_rates.

    The funnel now uses ``contact_extractor.count()`` (count-only search)
    instead of fetching full records, so the 502 fallback hooks into
    ``count`` rather than ``get_new_contacts``.
    """
    extractor = MagicMock()
    extractor.count.side_effect = Exception("502 Bad Gateway")

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


# ---------------------------------------------------------------------------
# Third bug-fix pass — three active bugs in the current GitHub code.
# ---------------------------------------------------------------------------


# --- Bug 9: activity SDK call missing object_type --------------------------
#
# ``client.search_objects("meetings", ...)`` used to fall through to the
# generic ``self.api.crm.objects.search_api.do_search`` with only the
# search request as a kwarg — but the generic endpoint requires
# ``object_type`` as a first positional argument, so every activity
# report crashed with ``TypeError: do_search() missing 1 required
# positional argument: 'object_type'``.


def test_search_objects_passes_object_type_for_generic_objects_api():
    """Activities route through ``crm.objects.search_api.do_search`` and
    must pass ``object_type`` as a positional argument."""
    from hubspot_revops.client import HubSpotClient

    client = HubSpotClient(access_token="test-token")
    mock_api = MagicMock()
    client.api = mock_api

    client.search_objects(
        object_type="meetings",
        filter_groups=[{"filters": []}],
        properties=["hs_timestamp"],
    )

    do_search = mock_api.crm.objects.search_api.do_search
    assert do_search.called, "generic objects search_api was never called"
    call = do_search.call_args
    # Positional args MUST include the object_type string.
    assert call.args == ("meetings",), (
        f"expected ('meetings',) positional args, got {call.args!r}"
    )
    assert "public_object_search_request" in call.kwargs


def test_search_objects_omits_object_type_for_typed_search_api():
    """Typed namespaces (contacts, deals, companies) already know their
    object type — passing it positionally would raise TypeError."""
    from hubspot_revops.client import HubSpotClient

    client = HubSpotClient(access_token="test-token")
    mock_api = MagicMock()
    client.api = mock_api

    client.search_objects(
        object_type="contacts",
        filter_groups=[{"filters": []}],
        properties=["email"],
    )

    do_search = mock_api.crm.contacts.search_api.do_search
    assert do_search.called
    call = do_search.call_args
    assert call.args == (), (
        f"typed search_api.do_search must not receive positional args, "
        f"got {call.args!r}"
    )
    assert "public_object_search_request" in call.kwargs


def test_search_objects_routes_custom_objects_through_generic_api():
    """Custom object types aren't in ``_sdk_module``'s mapping either,
    so they also hit the generic endpoint and need ``object_type``."""
    from hubspot_revops.client import HubSpotClient

    client = HubSpotClient(access_token="test-token")
    mock_api = MagicMock()
    client.api = mock_api

    client.search_objects(
        object_type="p1234567_orders",  # arbitrary custom object type
        filter_groups=[{"filters": []}],
    )

    do_search = mock_api.crm.objects.search_api.do_search
    assert do_search.call_args.args == ("p1234567_orders",)


# --- Bug 10: pipeline currency mixing in total_pipeline_value --------------
#
# ``pipeline.total_pipeline_value`` used to sum every open-deal amount
# into a single scalar regardless of ``deal_currency_code``. On a portal
# with JPY + USD deals a ¥990K deal was being reported as $990K of
# pipeline, silently inflating the total by the JPY→USD FX mismatch.


def test_total_pipeline_value_separates_jpy_and_usd():
    """Multi-currency open deals must bucket by currency and never sum
    into a single mixed scalar."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["990000", "5000", "3000"],
        "dealstage": ["qualified", "demo", "proposal"],
        "pipeline": ["japan", "default", "default"],
        "deal_currency_code": ["JPY", "USD", "USD"],
    })

    result = pipeline.total_pipeline_value(extractor)

    assert result["total_deals"] == 3
    assert set(result["by_currency"].keys()) == {"JPY", "USD"}
    assert result["by_currency"]["JPY"]["total_value"] == 990_000
    assert result["by_currency"]["JPY"]["deal_count"] == 1
    assert result["by_currency"]["JPY"]["avg_deal_size"] == 990_000
    assert result["by_currency"]["USD"]["total_value"] == 8_000
    assert result["by_currency"]["USD"]["deal_count"] == 2
    assert result["by_currency"]["USD"]["avg_deal_size"] == 4_000
    # Primary currency = whichever has the most deals (USD: 2).
    assert result["primary_currency"] == "USD"
    # Back-compat flat field reflects ONLY the primary currency, never a
    # cross-currency sum.
    assert result["total_value"] == 8_000
    assert result["avg_deal_size"] == 4_000


def test_total_pipeline_value_no_currency_column_defaults_to_usd():
    """Portals without a ``deal_currency_code`` property still work and
    produce a single-currency USD payload (back-compat with old tests)."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["10000", "20000", "30000"],
        "dealstage": ["qualified", "demo", "proposal"],
    })

    result = pipeline.total_pipeline_value(extractor)
    assert result["total_deals"] == 3
    assert result["total_value"] == 60_000
    assert result["avg_deal_size"] == 20_000
    assert result["primary_currency"] == "USD"
    assert result["by_currency"]["USD"]["total_value"] == 60_000


def test_total_pipeline_value_empty_payload_shape():
    """Empty extractor must return every key callers rely on, not the
    old ``{total_deals, total_value}`` pair — pipeline_velocity() and
    the templates expect ``avg_deal_size`` and ``by_currency`` keys too."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame()

    result = pipeline.total_pipeline_value(extractor)
    assert result["total_deals"] == 0
    assert result["total_value"] == 0.0
    assert result["avg_deal_size"] == 0.0
    assert result["by_currency"] == {}
    assert result["primary_currency"] == "USD"


# --- Bug 11: funnel contact 10K cap ----------------------------------------
#
# ``conversion.funnel_conversion_rates`` used to fetch every matching
# contact via ``contact_extractor.get_new_contacts``, which walks the
# ``after`` cursor up to the HubSpot Search API's 10,000-result hard
# cap. Portals with more than 10k new contacts in a period had every
# stage count silently clipped to whatever fit in the first 10k page.
# The fix switches to a count-only search (limit=1, response.total),
# which bypasses the pagination cap entirely.


def test_funnel_uses_count_only_search_bypassing_10k_cap():
    """50k contacts in range must be reported as 50k, not clipped to 10k."""
    extractor = MagicMock()

    def _count_side_effect(filter_groups):
        filters = filter_groups[0]["filters"]
        has_prop = next(
            (f["propertyName"] for f in filters if f.get("operator") == "HAS_PROPERTY"),
            None,
        )
        if has_prop is None:
            # Total contacts in range — deliberately above the 10k cap.
            return 50_000
        stage_totals = {
            "hs_lifecyclestage_lead_date": 40_000,
            "hs_lifecyclestage_marketingqualifiedlead_date": 12_000,
            "hs_lifecyclestage_salesqualifiedlead_date": 4_000,
            "hs_lifecyclestage_opportunity_date": 1_500,
            "hs_lifecyclestage_customer_date": 500,
        }
        return stage_totals[has_prop]

    extractor.count.side_effect = _count_side_effect

    result = conversion.funnel_conversion_rates(extractor, _tr())

    # Every stage count is the true total, not clipped to 10k.
    assert result["total_contacts"] == 50_000
    assert result["stages"]["subscriber"] == 50_000
    assert result["stages"]["lead"] == 40_000
    assert result["stages"]["marketingqualifiedlead"] == 12_000
    assert result["stages"]["salesqualifiedlead"] == 4_000
    assert result["stages"]["opportunity"] == 1_500
    assert result["stages"]["customer"] == 500
    # Step-wise conversion uses the uncapped totals.
    lead_to_mql = result["conversions"]["lead_to_marketingqualifiedlead"]
    assert lead_to_mql["from_count"] == 40_000
    assert lead_to_mql["to_count"] == 12_000
    assert lead_to_mql["conversion_rate"] == 30.0
    # Must NOT fall back to the paginated fetch that has the cap.
    extractor.get_new_contacts.assert_not_called()


def test_funnel_count_receives_createdate_range_on_every_call():
    """Every count() call must include the createdate time-range filter;
    otherwise stage totals span the entire portal history instead of the
    requested period."""
    extractor = MagicMock()
    extractor.count.return_value = 0

    tr = _tr()
    conversion.funnel_conversion_rates(extractor, tr)

    assert extractor.count.call_count >= 1
    for call in extractor.count.call_args_list:
        filter_groups = call.args[0] if call.args else call.kwargs["filter_groups"]
        filters = filter_groups[0]["filters"]
        prop_names = [f["propertyName"] for f in filters]
        assert prop_names.count("createdate") == 2, (
            "every count call must include both GTE and LTE createdate filters"
        )


def test_base_extractor_count_reads_response_total():
    """BaseExtractor.count must use limit=1 (no pagination) and return
    response.total as an int."""
    from types import SimpleNamespace

    from hubspot_revops.extractors.base import BaseExtractor

    client = MagicMock()
    client.search_objects.return_value = SimpleNamespace(total=50_000)

    extractor = BaseExtractor(client)
    extractor.object_type = "contacts"

    count = extractor.count([{"filters": []}])
    assert count == 50_000

    # Verify the call used limit=1, not 10000.
    kwargs = client.search_objects.call_args.kwargs
    assert kwargs["limit"] == 1
    assert kwargs["object_type"] == "contacts"


def test_base_extractor_count_missing_total_returns_zero():
    """If the SDK ever returns a response without a ``total`` attribute,
    count should return 0 rather than crash."""
    from types import SimpleNamespace

    from hubspot_revops.extractors.base import BaseExtractor

    client = MagicMock()
    client.search_objects.return_value = SimpleNamespace()  # no .total

    extractor = BaseExtractor(client)
    extractor.object_type = "contacts"

    assert extractor.count([{"filters": []}]) == 0


# --- Bug 12: avg_deal_size currency mixing ---------------------------------
#
# Sibling of Bug 10: ``pipeline.avg_deal_size`` used to average every won
# deal's amount regardless of ``deal_currency_code``. A ¥1,000,000 deal
# and a $50,000 deal produced a meaningless "$525,000 avg deal size".
# The fix mirrors total_pipeline_value — bucket by currency, expose
# by_currency + primary_currency, keep back-compat flat fields so
# pipeline_velocity() and the exec summary template still work.


def test_avg_deal_size_separates_jpy_and_usd():
    """Multi-currency won deals must never be averaged into a single scalar."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["1000000", "40000", "60000"],
        "hs_is_closed_won": ["true", "true", "true"],
        "pipeline": ["japan", "default", "default"],
        "deal_currency_code": ["JPY", "USD", "USD"],
    })

    result = pipeline.avg_deal_size(extractor, _tr())

    assert set(result["by_currency"].keys()) == {"JPY", "USD"}
    # JPY: one deal at 1M — avg == 1M
    assert result["by_currency"]["JPY"]["avg_deal_size"] == 1_000_000
    assert result["by_currency"]["JPY"]["total_revenue"] == 1_000_000
    assert result["by_currency"]["JPY"]["deal_count"] == 1
    # USD: two deals averaging 50k
    assert result["by_currency"]["USD"]["avg_deal_size"] == 50_000
    assert result["by_currency"]["USD"]["total_revenue"] == 100_000
    assert result["by_currency"]["USD"]["deal_count"] == 2
    # Primary = whichever has the most deals (USD: 2).
    assert result["primary_currency"] == "USD"
    # Back-compat flat field reflects primary currency only — never a
    # cross-currency average like the old (1_000_000+40_000+60_000)/3.
    assert result["avg_deal_size"] == 50_000
    assert result["total_revenue"] == 100_000
    assert result["deal_count"] == 2


def test_avg_deal_size_no_currency_column_defaults_to_usd():
    """Back-compat: portals without deal_currency_code still work."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "amount": ["10000", "20000"],
        "hs_is_closed_won": ["true", "true"],
    })

    result = pipeline.avg_deal_size(extractor, _tr())
    assert result["primary_currency"] == "USD"
    assert result["avg_deal_size"] == 15_000
    assert result["total_revenue"] == 30_000
    assert result["deal_count"] == 2
    assert result["by_currency"]["USD"]["avg_deal_size"] == 15_000


def test_avg_deal_size_empty_payload_shape():
    """Empty extractor must return every key callers rely on."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame()

    result = pipeline.avg_deal_size(extractor, _tr())
    assert result["avg_deal_size"] == 0.0
    assert result["total_revenue"] == 0.0
    assert result["deal_count"] == 0
    assert result["by_currency"] == {}
    assert result["primary_currency"] == "USD"


def test_pipeline_velocity_reads_back_compat_avg_deal_size():
    """pipeline_velocity() must still resolve ads['avg_deal_size'] to a
    scalar after the multi-currency refactor, otherwise the velocity
    formula crashes on multi-currency portals."""
    extractor = MagicMock()
    # Open deals for total_pipeline_value.
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "amount": ["5000", "7000"],
        "dealstage": ["qualified", "demo"],
        "pipeline": ["default", "default"],
        "deal_currency_code": ["USD", "USD"],
    })
    # The API-level ``won_only=True`` flag returns only won deals, so
    # the MagicMock must honour it — otherwise avg_deal_size sees the
    # loss row and the test asserts the wrong scalar. Simulate the two
    # server-side filter paths via ``side_effect``.
    all_closed = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "amount": ["1000000", "40000", "60000", "1000"],
        "hs_is_closed_won": ["true", "true", "true", "false"],
        "pipeline": ["japan", "default", "default", "default"],
        "deal_currency_code": ["JPY", "USD", "USD", "USD"],
        "createdate": ["2026-01-01", "2026-01-01", "2026-01-01", "2026-01-01"],
        "closedate": ["2026-02-01", "2026-02-01", "2026-02-01", "2026-02-01"],
    })
    won_only = all_closed[all_closed["hs_is_closed_won"] == "true"].reset_index(drop=True)

    def _closed_side_effect(time_range, won_only_flag=False, **_kwargs):
        return won_only if won_only_flag else all_closed

    # Match the real signature: (time_range, won_only=False, properties=None).
    extractor.get_closed_deals.side_effect = lambda *args, **kwargs: (
        won_only if kwargs.get("won_only") or (len(args) > 1 and args[1]) else all_closed
    )

    result = pipeline.pipeline_velocity(extractor, _tr())
    # velocity formula is (deals * win_pct * avg_size) / avg_cycle — the
    # important bit is that we don't crash pulling avg_deal_size from
    # the multi-currency payload, and that avg_size is the USD primary
    # scalar (50_000 = (40k+60k)/2), not a cross-currency mash-up.
    assert result["avg_deal_size"] == 50_000
    assert result["open_deals"] == 2
    assert result["win_rate"] == 75.0  # 3W / 1L across all currencies
