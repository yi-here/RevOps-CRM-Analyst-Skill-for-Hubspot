# HubSpot RevOps CRM Analyst Skill

## What This Skill Does

Connects to a HubSpot CRM instance and answers RevOps business questions by:
1. Auto-discovering the CRM schema (objects, properties, pipelines, associations)
2. Extracting relevant data via the HubSpot Search API
3. Computing standard RevOps metrics (pipeline, revenue, funnel, activity, team, forecast)
4. Generating markdown reports, tables, and charts

## Setup

1. Set `HUBSPOT_ACCESS_TOKEN` in your environment (Private App token)
2. Run `pip install -e .` from the project root
3. The skill auto-discovers your HubSpot schema on first run

## Architecture

- `hubspot_revops/client.py` — API client with auth, rate limiting, retry
- `hubspot_revops/schema/` — Schema discovery, caching, Pydantic models
- `hubspot_revops/extractors/` — Data extraction per object type
- `hubspot_revops/metrics/` — Metric computation engines
- `hubspot_revops/reports/` — Report generation (markdown, CSV, charts)
- `hubspot_revops/nl_interface.py` — Natural language → metric routing

## Key Commands

```bash
# Discover schema
python -m hubspot_revops.cli schema

# Run a report
python -m hubspot_revops.cli report pipeline
python -m hubspot_revops.cli report revenue --period Q2-2026
python -m hubspot_revops.cli report funnel

# Ask a question
python -m hubspot_revops.cli ask "What's our win rate this quarter?"

# Export data
python -m hubspot_revops.cli export deals --format csv
```

## Coding Guidelines

- Read-only access to HubSpot — never create/update/delete CRM records
- Use the Search API with filters rather than dumping all objects
- Always request only the properties needed for the metric
- Respect rate limits (100 req/10s free, 190 req/10s pro/enterprise)
- Cache schema for 24h, query results for 5 minutes
- All metrics functions should accept a time range parameter
- Return results as pandas DataFrames internally, format for output at the report layer

## Testing

```bash
pytest tests/ -v
```

Tests use mocked API responses from `tests/fixtures/`. No live HubSpot connection needed.
