"""Tests for metric computation."""

from unittest.mock import MagicMock, patch

import pandas as pd

from hubspot_revops.metrics import pipeline


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

    from hubspot_revops.extractors.base import TimeRange
    from datetime import datetime, timedelta

    tr = TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())
    result = pipeline.win_rate(extractor, tr)
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

    from hubspot_revops.extractors.base import TimeRange
    from datetime import datetime, timedelta

    tr = TimeRange(start=datetime.now() - timedelta(days=90), end=datetime.now())
    result = pipeline.win_rate(extractor, tr)
    assert result["win_rate"] == 60.0
    assert result["won"] == 3
    assert result["lost"] == 2
