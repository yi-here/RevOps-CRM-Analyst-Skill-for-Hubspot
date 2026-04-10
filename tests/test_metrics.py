"""Tests for metric computation."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.metrics import pipeline, team
from hubspot_revops.metrics._utils import to_bool_series, to_numeric_series
from hubspot_revops.schema.models import Owner


def _tr():
    return TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())


def test_total_pipeline_value_empty():
    """Test pipeline value with no deals."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame()

    result = pipeline.total_pipeline_value(extractor)
    assert result["total_deals"] == 0
    assert result["total_value"] == 0.0


def test_total_pipeline_value_with_deals():
    """Test pipeline value calculation."""
    extractor = MagicMock()
    extractor.get_open_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "amount": ["10000", "20000", "30000"],
        "dealstage": ["qualified", "demo", "proposal"],
    })

    result = pipeline.total_pipeline_value(extractor)
    assert result["total_deals"] == 3
    assert result["total_value"] == 60000.0
    assert result["avg_deal_size"] == 20000.0


def test_win_rate_empty():
    """Test win rate with no closed deals."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame()

    result = pipeline.win_rate(extractor, _tr())
    assert result["win_rate"] == 0.0
    assert result["total_closed"] == 0


def test_win_rate_calculation():
    """Test win rate with mixed won/lost deals."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3", "4", "5"],
        "hs_is_closed_won": ["true", "true", "true", "false", "false"],
        "amount": ["10000", "20000", "15000", "5000", "8000"],
    })

    result = pipeline.win_rate(extractor, _tr())
    assert result["win_rate"] == 60.0
    assert result["won"] == 3
    assert result["lost"] == 2


# ---------------------------------------------------------------------------
# Scalar bug regression: the reported crash was "AttributeError: 'int' object
# has no attribute 'fillna'" on line 29 of team.py. The literal trigger is a
# closed-deals DataFrame missing the ``amount`` column entirely — then
# ``df.get("amount", 0)`` returns the scalar 0 and ``.fillna(0)`` crashes.
# These tests reproduce both that shape and the empty-DataFrame case.
# ---------------------------------------------------------------------------


def test_to_numeric_series_empty_df():
    result = to_numeric_series(pd.DataFrame(), "amount")
    assert isinstance(result, pd.Series)
    assert len(result) == 0
    # An empty series sums to 0 and has NaN mean — this is what the callers
    # tolerate, so the contract is "never a scalar".
    assert result.sum() == 0


def test_to_numeric_series_missing_column():
    df = pd.DataFrame({"id": ["1", "2"]})
    result = to_numeric_series(df, "amount")
    assert isinstance(result, pd.Series)
    assert len(result) == 0


def test_to_numeric_series_happy_path():
    df = pd.DataFrame({"amount": ["100", "200", None]})
    result = to_numeric_series(df, "amount")
    assert list(result) == [100.0, 200.0, 0.0]


def test_to_bool_series_missing_column():
    result = to_bool_series(pd.DataFrame({"id": ["1"]}), "hs_is_closed_won")
    assert isinstance(result, pd.Series)
    assert result.dtype == bool
    assert len(result) == 0


def test_to_bool_series_mixed_case():
    df = pd.DataFrame({"hs_is_closed_won": ["True", "FALSE", "true", None]})
    result = to_bool_series(df, "hs_is_closed_won")
    assert list(result) == [True, False, True, False]


def test_rep_scorecard_missing_amount_column():
    """Regression: the original crash. Deals DataFrame missing ``amount``
    must not raise AttributeError; it should simply produce zero revenue."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["123"],
        "hs_is_closed_won": ["true"],
        # NOTE: no ``amount`` column at all
    })
    extractor.get_open_deals.return_value = pd.DataFrame()

    owners = {"123": Owner(owner_id="123", first_name="Alice", last_name="Smith")}
    df = team.rep_scorecard(extractor, _tr(), owners)

    assert not df.empty
    row = df.iloc[0]
    assert row["rep_name"] == "Alice Smith"
    assert row["closed_won_revenue"] == 0
    assert row["deals_won"] == 1


def test_rep_scorecard_multi_currency_separates_jpy_and_usd():
    """Multi-currency must produce one row per (rep, currency), not a
    single mixed total."""
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "hubspot_owner_id": ["123", "123", "123"],
        "hs_is_closed_won": ["true", "true", "true"],
        "amount": ["1000000", "500", "300"],
        "deal_currency_code": ["JPY", "USD", "USD"],
        "pipeline": ["japan", "default", "default"],
    })
    extractor.get_open_deals.return_value = pd.DataFrame()

    owners = {"123": Owner(owner_id="123", first_name="Alice", last_name="Smith")}
    df = team.rep_scorecard(extractor, _tr(), owners)

    assert set(df["currency"]) == {"JPY", "USD"}
    jpy = df[df["currency"] == "JPY"].iloc[0]
    usd = df[df["currency"] == "USD"].iloc[0]
    assert jpy["closed_won_revenue"] == 1_000_000
    assert usd["closed_won_revenue"] == 800
    assert jpy["deals_won"] == 1
    assert usd["deals_won"] == 2


def test_rep_scorecard_pipeline_filter():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "hubspot_owner_id": ["123", "123"],
        "hs_is_closed_won": ["true", "true"],
        "amount": ["1000", "9999"],
        "deal_currency_code": ["USD", "USD"],
        "pipeline": ["default", "japan"],
    })
    extractor.get_open_deals.return_value = pd.DataFrame()

    owners = {"123": Owner(owner_id="123", first_name="Alice", last_name="Smith")}
    df = team.rep_scorecard(extractor, _tr(), owners, pipeline_filter="japan")

    assert len(df) == 1
    assert df.iloc[0]["closed_won_revenue"] == 9999


def test_rep_scorecard_win_and_loss_rate():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "hubspot_owner_id": ["123", "123", "123", "123"],
        "hs_is_closed_won": ["true", "true", "false", "false"],
        "amount": ["1000", "2000", "500", "700"],
        "deal_currency_code": ["USD", "USD", "USD", "USD"],
        "pipeline": ["default", "default", "default", "default"],
    })
    extractor.get_open_deals.return_value = pd.DataFrame()

    owners = {"123": Owner(owner_id="123", first_name="Alice", last_name="Smith")}
    df = team.rep_scorecard(extractor, _tr(), owners)
    row = df.iloc[0]
    assert row["deals_won"] == 2
    assert row["deals_lost"] == 2
    assert row["win_rate"] == 50.0
    assert row["loss_rate"] == 50.0
    assert row["lost_revenue"] == 1200
    assert row["closed_won_revenue"] == 3000
