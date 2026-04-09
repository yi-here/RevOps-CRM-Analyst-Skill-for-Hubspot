"""Shared test fixtures and mock HubSpot API responses."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.schema.models import (
    CRMSchema,
    ObjectSchema,
    Owner,
    Pipeline,
    PipelineStage,
    PropertySchema,
)


@pytest.fixture
def time_range():
    """Default test time range: last 90 days."""
    end = datetime(2026, 3, 31)
    start = end - timedelta(days=90)
    return TimeRange(start=start, end=end)


@pytest.fixture
def mock_client():
    """A mocked HubSpotClient."""
    return MagicMock()


@pytest.fixture
def sample_schema():
    """A sample CRM schema for testing."""
    return CRMSchema(
        objects={
            "deals": ObjectSchema(
                name="deals",
                label="Deals",
                properties=[
                    PropertySchema(name="amount", label="Amount", type="number", field_type="number"),
                    PropertySchema(name="dealstage", label="Deal Stage", type="string", field_type="select"),
                    PropertySchema(name="closedate", label="Close Date", type="date", field_type="date"),
                    PropertySchema(name="pipeline", label="Pipeline", type="string", field_type="select"),
                    PropertySchema(name="hubspot_owner_id", label="Owner", type="string", field_type="text"),
                ],
            ),
            "contacts": ObjectSchema(
                name="contacts",
                label="Contacts",
                properties=[
                    PropertySchema(name="email", label="Email", type="string", field_type="text"),
                    PropertySchema(name="lifecyclestage", label="Lifecycle Stage", type="string", field_type="select"),
                ],
            ),
        },
        pipelines={
            "deals": [
                Pipeline(
                    pipeline_id="default",
                    label="Sales Pipeline",
                    display_order=0,
                    stages=[
                        PipelineStage(stage_id="qualified", label="Qualified", display_order=0, probability=20),
                        PipelineStage(stage_id="demo", label="Demo Scheduled", display_order=1, probability=40),
                        PipelineStage(stage_id="proposal", label="Proposal Sent", display_order=2, probability=60),
                        PipelineStage(stage_id="negotiation", label="Negotiation", display_order=3, probability=80),
                        PipelineStage(stage_id="closedwon", label="Closed Won", display_order=4, probability=100, is_closed=True, is_won=True),
                        PipelineStage(stage_id="closedlost", label="Closed Lost", display_order=5, probability=0, is_closed=True, is_won=False),
                    ],
                )
            ]
        },
        owners={
            "123": Owner(owner_id="123", email="alice@example.com", first_name="Alice", last_name="Smith"),
            "456": Owner(owner_id="456", email="bob@example.com", first_name="Bob", last_name="Jones"),
        },
    )
