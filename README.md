# RevOps CRM Analyst Skill for HubSpot

A Python skill that connects to HubSpot's CRM API, auto-discovers your schema, computes RevOps metrics, and answers natural-language business questions. Built for Claude Code, OpenClaw, and other LLM-powered agents.

## What It Does

1. **Connects** to HubSpot via Private App token (read-only)
2. **Discovers** your CRM schema — objects, properties, pipelines, associations
3. **Computes** 40+ RevOps metrics across pipeline, revenue, funnel, activity, and team performance
4. **Answers** natural-language questions like "What's our win rate by rep this quarter?"
5. **Generates** markdown reports, CSV exports, and charts

## Quick Start

```bash
# Install
pip install -e .

# Set your HubSpot Private App token
export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx

# Discover your schema
python -m hubspot_revops.cli schema

# Run a report
python -m hubspot_revops.cli report executive
python -m hubspot_revops.cli report pipeline --period Q1-2026

# Ask a question
python -m hubspot_revops.cli ask "Who are our top performing reps?"
```

## Metrics Catalog

| Category | Metrics |
|---|---|
| **Pipeline** | Total value, by stage, by rep, win rate, velocity, coverage, stage conversion |
| **Revenue** | Closed revenue, MRR/ARR, expansion, churn, NRR, by source, by product |
| **Funnel** | Lead→MQL→SQL→Opp→Customer conversion, lead source breakdown, time in stage |
| **Activity** | Calls, emails, meetings per deal/rep, response times, engagement scores |
| **Team** | Rep scorecard, quota attainment, win rate by rep, cycle length by rep |
| **Forecast** | Weighted pipeline, forecast categories, commit vs. best case |

## Architecture

See [PLAN.md](PLAN.md) for the full implementation plan.

```
hubspot_revops/
├── client.py              # API client with auth, rate limiting, retry
├── schema/                # Schema discovery, caching, models
├── extractors/            # Data extraction per object type
├── metrics/               # Metric computation engines
├── reports/               # Report generation (markdown, CSV, charts)
├── nl_interface.py        # Natural language → metric routing
└── cli.py                 # CLI entry point
```

## Requirements

- Python 3.10+
- HubSpot Private App with read-only CRM scopes
- See [.env.example](.env.example) for configuration

## Security

- **Read-only** — never creates, updates, or deletes CRM data
- Tokens via environment variables only (never committed)
- Minimum-scope Private App permissions
- Rate limit compliant (100-190 req/10s depending on tier)
