"""Pipeline + stage ID resolution helpers.

The HubSpot schema (``CRMSchema.pipelines``) already stores every pipeline's
stages with their ``is_won`` / ``is_closed`` flags. These helpers expose that
data in the shapes callers actually need:

- ``resolve_pipeline_id`` — case-insensitive label-or-id → pipeline_id,
  used by the ``--pipeline`` CLI flag and the natural-language interface.
- ``get_won_lost_stages`` — enumerate the won/lost stage IDs for a given
  pipeline (or all pipelines for an object type), for closed-lost reason
  grouping and any future stage-scoped metric.
- ``get_pipeline_stage_labels`` — a ``(pipeline_id, stage_id) → label`` map
  that keeps identical-labelled stages across pipelines distinct.

These helpers deliberately do NOT replace the ``hs_is_closed`` /
``hs_is_closed_won`` property filters used by ``get_closed_deals``: HubSpot
populates those booleans from each stage's metadata across every pipeline,
so property-based filtering is already pipeline-safe for closed-vs-open
classification. The real gap these helpers fill is label disambiguation
and pipeline filtering.
"""

from __future__ import annotations

from hubspot_revops.schema.models import CRMSchema


def resolve_pipeline_id(
    schema: CRMSchema, name_or_id: str | None, object_type: str = "deals"
) -> str | None:
    """Resolve a user-supplied pipeline name or ID to the canonical ID.

    Returns ``None`` if ``name_or_id`` is None, empty, or equal to ``"all"``.
    Matches on pipeline_id first (exact), then on label (case-insensitive).
    Unknown values return ``None`` so callers can decide whether to warn or
    fall back to "all pipelines".
    """
    if not name_or_id:
        return None
    target = name_or_id.strip()
    if not target or target.lower() == "all":
        return None

    pipelines = schema.pipelines.get(object_type, [])
    # Exact ID match first.
    for pl in pipelines:
        if pl.pipeline_id == target:
            return pl.pipeline_id
    # Case-insensitive label match.
    target_lower = target.lower()
    for pl in pipelines:
        if pl.label.lower() == target_lower:
            return pl.pipeline_id
    # Substring fallback (e.g. "japan" matches "Japan Sales Pipeline").
    for pl in pipelines:
        if target_lower in pl.label.lower():
            return pl.pipeline_id
    return None


def get_won_lost_stages(
    schema: CRMSchema,
    object_type: str = "deals",
    pipeline_id: str | None = None,
) -> dict[str, list[str]]:
    """Return ``{"won": [...], "lost": [...]}`` stage IDs.

    If ``pipeline_id`` is given, only that pipeline is inspected; otherwise
    all pipelines for the object type are unioned.
    """
    won: list[str] = []
    lost: list[str] = []
    for pl in schema.pipelines.get(object_type, []):
        if pipeline_id and pl.pipeline_id != pipeline_id:
            continue
        for s in pl.stages:
            if s.is_won:
                won.append(s.stage_id)
            elif s.is_closed:
                lost.append(s.stage_id)
    return {"won": won, "lost": lost}


def get_pipeline_stage_labels(
    schema: CRMSchema, object_type: str = "deals"
) -> dict[tuple[str, str], tuple[str, str]]:
    """Return ``(pipeline_id, stage_id) → (stage_label, pipeline_label)``.

    Keeps identical-labelled stages across pipelines distinct, unlike the
    flat ``extractors/pipelines.py::get_stage_map`` helper which collapses
    them.
    """
    result: dict[tuple[str, str], tuple[str, str]] = {}
    for pl in schema.pipelines.get(object_type, []):
        for s in pl.stages:
            result[(pl.pipeline_id, s.stage_id)] = (s.label, pl.label)
    return result
