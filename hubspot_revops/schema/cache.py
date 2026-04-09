"""Schema caching — persist discovered schema to avoid repeated API calls."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from hubspot_revops.schema.models import CRMSchema

DEFAULT_CACHE_PATH = ".hubspot_schema_cache.json"
DEFAULT_TTL_SECONDS = 86400  # 24 hours


def load_cached_schema(
    cache_path: str = DEFAULT_CACHE_PATH,
    ttl_seconds: int | None = None,
) -> CRMSchema | None:
    """Load schema from cache if it exists and is fresh."""
    ttl = ttl_seconds or int(os.environ.get("HUBSPOT_SCHEMA_CACHE_TTL", DEFAULT_TTL_SECONDS))
    path = Path(cache_path)
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text())
        schema = CRMSchema.model_validate(data)
        age = datetime.now() - schema.generated_at
        if age < timedelta(seconds=ttl):
            return schema
    except Exception:
        pass
    return None


def save_schema_cache(schema: CRMSchema, cache_path: str = DEFAULT_CACHE_PATH) -> None:
    """Persist schema to disk."""
    path = Path(cache_path)
    path.write_text(schema.model_dump_json(indent=2))


def get_or_discover_schema(client, cache_path: str = DEFAULT_CACHE_PATH, force_refresh: bool = False) -> CRMSchema:
    """Load schema from cache or discover it fresh."""
    if not force_refresh:
        cached = load_cached_schema(cache_path)
        if cached is not None:
            return cached

    from hubspot_revops.schema.discovery import discover_schema

    schema = discover_schema(client)
    save_schema_cache(schema, cache_path)
    return schema
