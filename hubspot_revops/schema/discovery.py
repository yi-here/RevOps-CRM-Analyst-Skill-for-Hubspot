"""Schema introspection — discovers objects, properties, pipelines, associations, owners."""

from __future__ import annotations

from datetime import datetime

from hubspot_revops.client import HubSpotClient
from hubspot_revops.schema.models import (
    AssociationDef,
    CRMSchema,
    ObjectSchema,
    Owner,
    Pipeline,
    PipelineStage,
    PropertySchema,
)

STANDARD_OBJECTS = ["contacts", "companies", "deals", "tickets", "line_items", "products", "quotes"]
PIPELINED_OBJECTS = ["deals", "tickets"]


def discover_schema(client: HubSpotClient) -> CRMSchema:
    """Run full schema discovery against a HubSpot portal."""
    schema = CRMSchema(generated_at=datetime.now())

    # 1. Standard objects
    for obj_type in STANDARD_OBJECTS:
        obj_schema = _discover_object(client, obj_type, is_custom=False)
        schema.objects[obj_type] = obj_schema

    # 2. Custom objects
    try:
        custom_schemas = client.get_schemas()
        for cs in custom_schemas.results:
            obj_type = cs.name
            obj_schema = _discover_object(client, obj_type, is_custom=True, label=cs.labels.singular)
            schema.objects[obj_type] = obj_schema
    except Exception:
        pass  # Custom objects may not be available on all tiers

    # 3. Pipelines
    for obj_type in PIPELINED_OBJECTS:
        try:
            pipelines_resp = client.get_pipelines(obj_type)
            schema.pipelines[obj_type] = [
                Pipeline(
                    pipeline_id=p.id,
                    label=p.label,
                    display_order=p.display_order,
                    stages=[
                        PipelineStage(
                            stage_id=s.id,
                            label=s.label,
                            display_order=s.display_order,
                            probability=float(getattr(s.metadata, "probability", 0) or 0),
                            is_closed=getattr(s.metadata, "is_closed", False) or False,
                            is_won=getattr(s.metadata, "is_won", False) or False,
                        )
                        for s in sorted(p.stages, key=lambda s: s.display_order)
                    ],
                )
                for p in pipelines_resp.results
            ]
        except Exception:
            pass

    # 4. Owners
    try:
        owners_resp = client.get_owners(limit=100)
        for o in owners_resp.results:
            schema.owners[o.id] = Owner(
                owner_id=o.id,
                email=getattr(o, "email", ""),
                first_name=getattr(o, "first_name", ""),
                last_name=getattr(o, "last_name", ""),
            )
    except Exception:
        pass

    # 5. Association definitions (standard pairs)
    association_pairs = [
        ("deals", "contacts"),
        ("deals", "companies"),
        ("contacts", "companies"),
        ("deals", "line_items"),
        ("tickets", "contacts"),
        ("tickets", "companies"),
    ]
    for from_obj, to_obj in association_pairs:
        schema.associations.append(
            AssociationDef(from_object=from_obj, to_object=to_obj)
        )

    return schema


def _discover_object(
    client: HubSpotClient, obj_type: str, is_custom: bool, label: str | None = None
) -> ObjectSchema:
    """Discover properties for a single object type."""
    properties = []
    try:
        props_resp = client.get_properties(obj_type)
        for p in props_resp.results:
            properties.append(
                PropertySchema(
                    name=p.name,
                    label=p.label,
                    type=p.type,
                    field_type=p.field_type,
                    group_name=getattr(p, "group_name", ""),
                    options=[
                        {"label": o.label, "value": o.value}
                        for o in (p.options or [])
                    ],
                    calculated=getattr(p, "calculated", False) or False,
                    has_unique_value=getattr(p, "has_unique_value", False) or False,
                    description=getattr(p, "description", "") or "",
                )
            )
    except Exception:
        pass

    return ObjectSchema(
        name=obj_type,
        label=label or obj_type.replace("_", " ").title(),
        is_custom=is_custom,
        properties=properties,
    )
