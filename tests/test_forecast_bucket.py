"""Tests for the forecast bucket assignment."""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd

from hubspot_revops.metrics import forecast_bucket
from hubspot_revops.schema.models import Owner


def _owners():
    return {
        "A": Owner(owner_id="A", first_name="Alice", last_name="Smith"),
    }


def _mock_extractor(df: pd.DataFrame) -> MagicMock:
    extractor = MagicMock()
    extractor.search_in_time_range.return_value = df
    return extractor


def test_bucket_commit_high_probability():
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
    extractor = _mock_extractor(df)

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {"stage_id": "negotiation", "label": "Negotiation", "probability": 0.8})(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    per_rep = data["per_rep"]
    assert not per_rep.empty
    assert per_rep.iloc[0]["bucket"] == "Commit"


def test_bucket_commit_via_contract_sent_and_next_step():
    df = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["A"],
        "amount": ["10000"],
        "dealstage": ["contract_sent"],
        "deal_currency_code": ["USD"],
        "pipeline": ["default"],
        "hs_forecast_category": [""],
        "hs_next_step": ["Customer reviewing MSA"],
    })
    extractor = _mock_extractor(df)

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                # Use a below-0.8 probability so the stage-label rule is
                # what moves this into Commit.
                type("S", (), {"stage_id": "contract_sent", "label": "Contract Sent", "probability": 0.6})(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    assert data["per_rep"].iloc[0]["bucket"] == "Commit"


def test_bucket_highly_likely_mid_probability():
    # Use a neutral stage label here — "Proposal" / "Contract" are
    # explicit Commit triggers under the current rules, so we pick
    # "Evaluation" to isolate the probability-based path (≥0.5).
    df = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["A"],
        "amount": ["5000"],
        "dealstage": ["evaluation"],
        "deal_currency_code": ["USD"],
        "pipeline": ["default"],
        "hs_forecast_category": [""],
        "hs_next_step": [""],
    })
    extractor = _mock_extractor(df)

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {"stage_id": "evaluation", "label": "Evaluation", "probability": 0.6})(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    assert data["per_rep"].iloc[0]["bucket"] == "Highly Likely"


def test_bucket_best_case_low_probability():
    df = pd.DataFrame({
        "id": ["1"],
        "hubspot_owner_id": ["A"],
        "amount": ["1000"],
        "dealstage": ["qualified"],
        "deal_currency_code": ["USD"],
        "pipeline": ["default"],
        "hs_forecast_category": [""],
        "hs_next_step": [""],
    })
    extractor = _mock_extractor(df)

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {"stage_id": "qualified", "label": "Qualified", "probability": 0.2})(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    assert data["per_rep"].iloc[0]["bucket"] == "Best Case"


def test_bucket_multi_currency_separate_totals():
    df = pd.DataFrame({
        "id": ["1", "2"],
        "hubspot_owner_id": ["A", "A"],
        "amount": ["10000", "1000000"],
        "dealstage": ["negotiation", "negotiation"],
        "deal_currency_code": ["USD", "JPY"],
        "pipeline": ["default", "japan"],
        "hs_forecast_category": ["", ""],
        "hs_next_step": ["", ""],
    })
    extractor = _mock_extractor(df)

    class FakeSchema:
        pipelines = {"deals": [type("P", (), {
            "pipeline_id": "default",
            "stages": [
                type("S", (), {"stage_id": "negotiation", "label": "Negotiation", "probability": 0.8})(),
            ],
        })()]}

    data = forecast_bucket.month_forecast_buckets(
        extractor, FakeSchema(), owners=_owners(), now=datetime(2026, 4, 10)
    )
    totals = data["totals_by_bucket"]
    usd_row = totals[totals["currency"] == "USD"].iloc[0]
    jpy_row = totals[totals["currency"] == "JPY"].iloc[0]
    assert usd_row["value"] == 10000
    assert jpy_row["value"] == 1_000_000
    assert "USD" in data["currencies"]
    assert "JPY" in data["currencies"]
