"""Tests for schema models and discovery."""

from hubspot_revops.schema.models import CRMSchema, ObjectSchema, Pipeline, PipelineStage, PropertySchema


def test_schema_summary(sample_schema):
    """Test that schema summary generates readable output."""
    summary = sample_schema.summary()
    assert "Deals" in summary
    assert "Contacts" in summary
    assert "Sales Pipeline" in summary
    assert "Qualified" in summary
    assert "Owners: 2" in summary


def test_property_schema():
    prop = PropertySchema(name="amount", label="Amount", type="number", field_type="number")
    assert prop.name == "amount"
    assert prop.type == "number"


def test_pipeline_stage_defaults():
    stage = PipelineStage(stage_id="test", label="Test", display_order=0)
    assert stage.probability == 0.0
    assert stage.is_closed is False
    assert stage.is_won is False


def test_crm_schema_empty():
    schema = CRMSchema()
    assert schema.objects == {}
    assert schema.pipelines == {}
    summary = schema.summary()
    assert "Owners: 0" in summary
