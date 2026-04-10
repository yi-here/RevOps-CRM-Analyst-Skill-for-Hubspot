# HubSpot RevOps CRM Analyst Skill

## What This Skill Does

Connects to a HubSpot CRM instance and answers RevOps business questions by:
1. Auto-discovering the CRM schema (objects, properties, pipelines, associations)
2. Extracting relevant data via the HubSpot Search API
3. Computing standard RevOps metrics (pipeline, revenue, funnel, activity, team, forecast)
4. Generating markdown reports, tables, and charts

## Setup

1. Register a HubSpot public app at https://developers.hubspot.com. Set its
   redirect URL to `http://localhost:8976/callback` and enable the read
   scopes listed in `README.md`.
2. Set `HUBSPOT_CLIENT_ID` and `HUBSPOT_CLIENT_SECRET` in your environment.
3. Run `pip install -e .` from the project root.
4. On first run, a browser opens to complete HubSpot authorization. Tokens
   are cached under `~/.hubspot_revops/tokens.json` and refreshed silently.
5. The skill auto-discovers your HubSpot schema on first run.

For CI or headless environments, set `HUBSPOT_ACCESS_TOKEN` instead (Private
App token or pre-minted OAuth token) to skip the interactive flow entirely.

## Architecture

- `hubspot_revops/client.py` — API client with auth, rate limiting, retry
- `hubspot_revops/schema/` — Schema discovery, caching, Pydantic models
- `hubspot_revops/extractors/` — Data extraction per object type
- `hubspot_revops/metrics/` — Metric computation engines
- `hubspot_revops/reports/` — Report generation (markdown, CSV, charts)
- `hubspot_revops/nl_interface.py` — Natural language → metric routing
- HubSpot's official MCP server (`https://mcp.hubspot.com/anthropic`) is the
  recommended companion for ad-hoc record lookups; this skill owns the
  aggregated RevOps metrics. Install with `claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic`.

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
