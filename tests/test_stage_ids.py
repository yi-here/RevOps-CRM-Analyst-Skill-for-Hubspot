"""Tests for pipeline + stage ID resolution helpers."""

from hubspot_revops.schema.stage_ids import (
    get_pipeline_stage_labels,
    get_won_lost_stages,
    resolve_pipeline_id,
)


def test_resolve_pipeline_id_none_and_all(sample_schema):
    assert resolve_pipeline_id(sample_schema, None) is None
    assert resolve_pipeline_id(sample_schema, "") is None
    assert resolve_pipeline_id(sample_schema, "all") is None
    assert resolve_pipeline_id(sample_schema, "ALL") is None


def test_resolve_pipeline_id_exact_id(sample_schema):
    assert resolve_pipeline_id(sample_schema, "default") == "default"
    assert resolve_pipeline_id(sample_schema, "japan") == "japan"


def test_resolve_pipeline_id_label_case_insensitive(sample_schema):
    assert resolve_pipeline_id(sample_schema, "Sales Pipeline") == "default"
    assert resolve_pipeline_id(sample_schema, "sales pipeline") == "default"
    assert resolve_pipeline_id(sample_schema, "JAPAN SALES PIPELINE") == "japan"


def test_resolve_pipeline_id_substring_fallback(sample_schema):
    # "japan" substring matches "Japan Sales Pipeline"
    assert resolve_pipeline_id(sample_schema, "japan") == "japan"


def test_resolve_pipeline_id_unknown(sample_schema):
    assert resolve_pipeline_id(sample_schema, "nonexistent") is None


def test_get_won_lost_stages_specific_pipeline(sample_schema):
    result = get_won_lost_stages(sample_schema, pipeline_id="japan")
    assert result["won"] == ["japan_closedwon"]
    assert result["lost"] == ["japan_closedlost"]


def test_get_won_lost_stages_all_pipelines(sample_schema):
    result = get_won_lost_stages(sample_schema)
    assert set(result["won"]) == {"closedwon", "japan_closedwon"}
    assert set(result["lost"]) == {"closedlost", "japan_closedlost"}


def test_get_pipeline_stage_labels_disambiguates(sample_schema):
    labels = get_pipeline_stage_labels(sample_schema)
    # Both pipelines have a "Qualified" stage but with different IDs —
    # they must stay distinct in the per-pipeline map.
    assert labels[("default", "qualified")] == ("Qualified", "Sales Pipeline")
    assert labels[("japan", "japan_qualified")] == ("Qualified", "Japan Sales Pipeline")
