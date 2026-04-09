"""Pydantic models representing HubSpot CRM schema elements."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class PropertySchema(BaseModel):
    """A single property on a CRM object."""

    name: str
    label: str
    type: str  # string, number, date, datetime, enumeration, bool
    field_type: str  # text, textarea, number, select, checkbox, date, etc.
    group_name: str = ""
    options: list[dict] = []  # For enumeration types: [{label, value, display_order}]
    calculated: bool = False
    has_unique_value: bool = False
    description: str = ""


class PipelineStage(BaseModel):
    """A stage within a pipeline."""

    stage_id: str
    label: str
    display_order: int
    probability: float = 0.0  # Deal win probability at this stage
    is_closed: bool = False
    is_won: bool = False


class Pipeline(BaseModel):
    """A CRM pipeline (deals, tickets, or custom objects)."""

    pipeline_id: str
    label: str
    display_order: int
    stages: list[PipelineStage] = []


class AssociationDef(BaseModel):
    """An association definition between two object types."""

    from_object: str
    to_object: str
    label: str = ""
    association_type_id: int = 0


class Owner(BaseModel):
    """A HubSpot owner (user)."""

    owner_id: str
    email: str = ""
    first_name: str = ""
    last_name: str = ""
    teams: list[str] = []

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip() or self.email


class ObjectSchema(BaseModel):
    """Full schema for a single CRM object type."""

    name: str
    label: str
    is_custom: bool = False
    properties: list[PropertySchema] = []
    primary_display_property: str = ""
    searchable_properties: list[str] = []


class CRMSchema(BaseModel):
    """Complete discovered schema for a HubSpot portal."""

    objects: dict[str, ObjectSchema] = {}
    pipelines: dict[str, list[Pipeline]] = {}  # objectType → pipelines
    associations: list[AssociationDef] = []
    owners: dict[str, Owner] = {}  # ownerId → Owner
    generated_at: datetime = datetime.now()

    def summary(self) -> str:
        """Generate a human-readable schema summary for LLM context."""
        lines = ["# CRM Schema Summary\n"]
        for name, obj in self.objects.items():
            prop_count = len(obj.properties)
            prop_names = ", ".join(p.name for p in obj.properties[:10])
            suffix = ", ..." if prop_count > 10 else ""
            custom_tag = " [CUSTOM]" if obj.is_custom else ""
            lines.append(f"## {obj.label}{custom_tag}")
            lines.append(f"  - {prop_count} properties: {prop_names}{suffix}")
            if name in self.pipelines:
                for pl in self.pipelines[name]:
                    stage_names = " → ".join(s.label for s in pl.stages)
                    lines.append(f"  - Pipeline '{pl.label}': {stage_names}")
        lines.append(f"\n## Owners: {len(self.owners)}")
        lines.append(f"## Associations: {len(self.associations)}")
        for assoc in self.associations[:20]:
            lines.append(f"  - {assoc.from_object} ↔ {assoc.to_object}")
        return "\n".join(lines)
