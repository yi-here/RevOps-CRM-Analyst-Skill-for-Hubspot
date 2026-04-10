"""Tests for the closed-lost analysis."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.metrics import closed_lost
from hubspot_revops.schema.models import Owner


def _tr():
    return TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())


def _owners():
    return {
        "A": Owner(owner_id="A", first_name="Alice", last_name="Smith"),
        "B": Owner(owner_id="B", first_name="Bob", last_name="Jones"),
    }


def test_closed_lost_empty():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame()

    data = closed_lost.closed_lost_analysis(extractor, _tr(), _owners())
    assert data["total_lost_deals"] == 0
    assert data["rep_scorecard"].empty
    assert data["reason_breakdown"].empty


def test_closed_lost_only_won_deals():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2"],
        "hubspot_owner_id": ["A", "B"],
        "hs_is_closed_won": ["true", "true"],
        "amount": ["100", "200"],
        "closed_lost_reason": ["", ""],
    })

    data = closed_lost.closed_lost_analysis(extractor, _tr(), _owners())
    assert data["total_lost_deals"] == 0


def test_closed_lost_rep_scorecard_and_reasons():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "hubspot_owner_id": ["A", "A", "B", "B"],
        "hs_is_closed_won": ["false", "false", "false", "false"],
        "amount": ["100", "300", "500", "200"],
        "closed_lost_reason": ["Price", "Price", "Competitor", ""],
    })
    # Mock engagement associations — one deal has no engagements (ghost).
    extractor.get_associated_ids.return_value = {"1": ["m1"], "2": ["m2"], "3": ["m3"], "4": []}

    data = closed_lost.closed_lost_analysis(extractor, _tr(), _owners())

    assert data["total_lost_deals"] == 4
    assert data["total_lost_value"] == 1100

    rep = data["rep_scorecard"]
    alice_row = rep[rep["rep_name"] == "Alice Smith"].iloc[0]
    bob_row = rep[rep["rep_name"] == "Bob Jones"].iloc[0]
    assert alice_row["deals_lost"] == 2
    assert alice_row["lost_value"] == 400
    assert bob_row["deals_lost"] == 2
    assert bob_row["lost_value"] == 700

    reasons = data["reason_breakdown"]
    assert "Price" in reasons["closed_lost_reason"].values
    assert "Competitor" in reasons["closed_lost_reason"].values
    assert "(no reason)" in reasons["closed_lost_reason"].values


def test_closed_lost_coverage_warning_when_majority_blank():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "hubspot_owner_id": ["A", "A", "A"],
        "hs_is_closed_won": ["false", "false", "false"],
        "amount": ["100", "200", "300"],
        "closed_lost_reason": ["", "", "Price"],
    })
    extractor.get_associated_ids.return_value = {"1": [], "2": [], "3": []}

    data = closed_lost.closed_lost_analysis(extractor, _tr(), _owners())
    assert data["coverage_warning"] is True
    # 1 of 3 has a reason → coverage ~= 0.33
    assert data["lost_reason_coverage"] < 0.5


def test_closed_lost_ghost_deal_count():
    extractor = MagicMock()
    extractor.get_closed_deals.return_value = pd.DataFrame({
        "id": ["1", "2", "3"],
        "hubspot_owner_id": ["A", "A", "A"],
        "hs_is_closed_won": ["false", "false", "false"],
        "amount": ["100", "200", "300"],
        "closed_lost_reason": ["Price", "Competitor", "Price"],
    })
    # Only deal 1 has any engagements; 2 and 3 are ghosts.
    extractor.get_associated_ids.return_value = {"1": ["m1"], "2": [], "3": []}

    data = closed_lost.closed_lost_analysis(extractor, _tr(), _owners())
    assert data["ghost_deal_count"] == 2
