---
name: hubspot-report
description: Generate RevOps reports from HubSpot CRM data — pipeline, revenue, funnel, team scorecard, activity, or executive summary. Use when the user asks about pipeline health, revenue, win rates, rep performance, or business metrics.
argument-hint: [report-type] [--period PERIOD]
allowed-tools: Bash(python *) Read
---

# Generate HubSpot RevOps Report

Generate a RevOps analytics report from HubSpot CRM data.

**Report type requested:** $ARGUMENTS

## Available Report Types

- `executive` — Full executive summary (pipeline + revenue + forecast)
- `pipeline` — Pipeline analysis: value by stage, win rate, velocity, coverage
- `revenue` — Closed revenue, MRR/ARR, revenue by rep/pipeline
- `funnel` — Lead→MQL→SQL→Opp→Customer conversion rates, lead sources
- `team` — Rep scorecard: pipeline, won revenue, win rate per rep
- `activity` — Engagement metrics: calls, emails, meetings by rep

## Steps

1. Ensure `HUBSPOT_CLIENT_ID` and `HUBSPOT_CLIENT_SECRET` are set (from a HubSpot public app registered at https://developers.hubspot.com). The first run will open a browser to complete authorization; subsequent runs use the cached refresh token. If the variables are missing, ask the user. For CI/headless, `HUBSPOT_ACCESS_TOKEN` can be set instead to skip the OAuth flow.
2. Parse the report type from `$ARGUMENTS`. Default to `executive` if unclear.
3. Parse the time period if provided (e.g., `Q1-2026`, `90d`, `6m`). Default to last 90 days.
4. Run the report:

```bash
cd $PROJECT_DIR && python -m hubspot_revops.cli report <type> --period <period>
```

5. Present the formatted markdown report to the user.
6. **If the output contains `FALLBACK_TO_MCP`**, the report method
   crashed at runtime (HubSpot 5xx, schema mismatch, or similar). Do
   NOT retry the same CLI command. Instead, hand off to HubSpot's
   official MCP server:
   - Read the error string inside the banner to understand what failed
   - Pick the right MCP tool for the equivalent raw-records query
     (`search_deals`, `search_contacts`, `get_engagements`, etc.)
   - Assemble the filters / time range the original report would have
     used and run the MCP query directly
   - Synthesize the answer from the raw records and explain the
     workaround to the user
7. Offer follow-up analysis options (drill into specific metrics,
   compare periods, etc.)

If dependencies are not installed, first run:
```bash
cd $PROJECT_DIR && pip install -e .
```
