"""Pipeline and stage metadata extraction."""

from __future__ import annotations

from hubspot_revops.client import HubSpotClient
from hubspot_revops.schema.models import Pipeline, PipelineStage


def get_deal_pipelines(client: HubSpotClient) -> list[Pipeline]:
    """Fetch all deal pipelines with stages."""
    response = client.get_pipelines("deals")
    pipelines = []
    for p in response.results:
        stages = [
            PipelineStage(
                stage_id=s.id,
                label=s.label,
                display_order=s.display_order,
                probability=float(getattr(s.metadata, "probability", 0) or 0),
                is_closed=getattr(s.metadata, "is_closed", False) or False,
                is_won=getattr(s.metadata, "is_won", False) or False,
            )
            for s in sorted(p.stages, key=lambda s: s.display_order)
        ]
        pipelines.append(
            Pipeline(
                pipeline_id=p.id,
                label=p.label,
                display_order=p.display_order,
                stages=stages,
            )
        )
    return pipelines


def get_stage_map(pipelines: list[Pipeline]) -> dict[str, PipelineStage]:
    """Build a mapping from stage_id → PipelineStage for quick lookup."""
    stage_map = {}
    for p in pipelines:
        for s in p.stages:
            stage_map[s.stage_id] = s
    return stage_map
