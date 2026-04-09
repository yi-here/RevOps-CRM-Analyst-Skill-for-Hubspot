# RevOps CRM Analyst Skill for HubSpot

<p align="center">
  <img src="https://flashlabs.ai/images/flashclaw-logo.png" alt="FlashClaw Logo" width="200"/>
</p>

<p align="center">
  <strong>Built for <a href="https://flashlabs.ai">FlashClaw</a> by <a href="https://flashlabs.ai">FlashLabs.ai</a></strong><br/>
  Enterprise-Ready GTM Intelligence &bull; OpenClaw &bull; Claude Code
</p>

<p align="center">
  <a href="#use-with-flashclaw--openclaw">FlashClaw / OpenClaw</a> &bull;
  <a href="#use-with-claude-code">Claude Code</a> &bull;
  <a href="#metrics-catalog">40+ Metrics</a> &bull;
  <a href="#quick-start">Quick Start</a>
</p>

---

## Why This Exists

Revenue Operations teams spend hours pulling data from HubSpot, building spreadsheets, and answering the same questions every week:

- *"What's our pipeline looking like?"*
- *"Who are the top-performing reps this quarter?"*
- *"What's our win rate trend?"*
- *"Show me the full funnel conversion rates."*
- *"Which lead sources are driving the most revenue?"*

**This skill turns your AI agent into a RevOps analyst.** Ask questions in plain English, get instant answers backed by live HubSpot data — no dashboards to build, no reports to schedule, no BI tools to configure.

---

## What It Does

```
┌──────────────────────────────────────────────────────────────────┐
│  "What's our win rate by rep this quarter?"                      │
└──────────────────────┬───────────────────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   AI Agent (Claude /    │
          │   OpenClaw / FlashClaw) │
          └────────────┬────────────┘
                       │ invokes skill
          ┌────────────▼────────────┐
          │  HubSpot RevOps Skill   │
          │                         │
          │  1. Connect to HubSpot  │
          │  2. Discover schema     │
          │  3. Query CRM data      │
          │  4. Compute metrics     │
          │  5. Format report       │
          └────────────┬────────────┘
                       │
          ┌────────────▼──────────────────────────────────────────┐
          │  Rep Scorecard — Q1 2026                               │
          │                                                        │
          │  | Rep     | Won Revenue | Win Rate | Avg Size |       │
          │  |---------|-------------|----------|----------|       │
          │  | Alice S | $485,000    | 42%      | $62K     |       │
          │  | Bob J   | $320,000    | 35%      | $48K     |       │
          │  | Carol M | $290,000    | 38%      | $55K     |       │
          └────────────────────────────────────────────────────────┘
```

1. **Connects** to HubSpot via Private App token (read-only, secure)
2. **Auto-discovers** your CRM schema — objects, properties, pipelines, associations
3. **Computes 40+ RevOps metrics** across pipeline, revenue, funnel, activity, and team
4. **Answers natural-language questions** — no SQL, no code, just ask
5. **Generates reports** — markdown tables, CSV exports, and charts

---

## Use with FlashClaw / OpenClaw

> **[FlashClaw](https://flashlabs.ai)** is the hosted, secure, enterprise-ready version of OpenClaw — built by [FlashLabs.ai](https://flashlabs.ai) for GTM teams that need production-grade AI agents with SOC 2 compliance, SSO, team management, and managed infrastructure. No self-hosting required.
>
> **[Get started with FlashClaw →](https://flashlabs.ai)**

### Install the Skill

**Option 1: From ClawHub (recommended)**
```bash
openclaw skill install hubspot-revops
```

**Option 2: From this repository**
```bash
# Clone into your skills directory
git clone https://github.com/yi-here/RevOps-CRM-Analyst-Skill-for-Hubspot.git \
  ~/.openclaw/skills/hubspot-revops

# Or for FlashClaw, add via the dashboard or:
flashclaw skill install hubspot-revops
```

**Option 3: Workspace-level**
```bash
# Copy the skill directory into your project
cp -r skills/hubspot-revops /your/workspace/.agents/skills/
```

### Configure

Set your HubSpot token in your OpenClaw/FlashClaw environment:

```bash
# OpenClaw
export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx

# FlashClaw — use the secrets dashboard or:
flashclaw secret set HUBSPOT_ACCESS_TOKEN pat-na1-xxxxxxxx
```

### Use It

Once installed, just talk to your agent:

```
You: What's our pipeline looking like?
Agent: [Connects to HubSpot, runs pipeline report, returns formatted results]

You: Who are the top reps this quarter?
Agent: [Generates rep scorecard with revenue, win rate, deal size per rep]

You: Compare Q1 vs Q2 revenue
Agent: [Runs revenue reports for both periods, shows comparison]
```

Or invoke directly:
```
/hubspot-revops pipeline
/hubspot-revops "What's our win rate?"
```

### Skill File Structure

```
skills/hubspot-revops/
├── SKILL.md              # Skill definition (OpenClaw/FlashClaw format)
└── scripts/
    └── install.sh        # Dependency installer
```

The `SKILL.md` declares `HUBSPOT_ACCESS_TOKEN` as a required env var, `python3` as a required binary, and includes full instructions for the agent to discover schema, run reports, and interpret results.

---

## Use with Claude Code

This repo ships with three Claude Code slash commands:

| Command | What It Does |
|---|---|
| `/hubspot-schema` | Discover your CRM schema — objects, properties, pipelines |
| `/hubspot-report [type] [--period]` | Generate a report: `pipeline`, `revenue`, `funnel`, `team`, `activity`, `executive` |
| `/hubspot-ask [question]` | Ask any business question in plain English |

### Setup

```bash
# 1. Clone the repo (the .claude/commands/ directory is included)
git clone https://github.com/yi-here/RevOps-CRM-Analyst-Skill-for-Hubspot.git
cd RevOps-CRM-Analyst-Skill-for-Hubspot

# 2. Install dependencies
pip install -e .

# 3. Set your HubSpot token
export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx

# 4. Open Claude Code in this directory
claude
```

### Examples

```
> /hubspot-schema
  → Shows all CRM objects, properties, pipelines, owners

> /hubspot-report executive
  → Full executive summary with pipeline, revenue, forecast

> /hubspot-report pipeline --period Q1-2026
  → Pipeline breakdown by stage for Q1

> /hubspot-ask "What's our average sales cycle by deal size?"
  → Computes and returns the answer

> /hubspot-ask "Show me deals stuck in negotiation for more than 30 days"
  → Finds and lists at-risk deals
```

### Skill File Structure

```
.claude/commands/
├── hubspot-schema.md     # /hubspot-schema command
├── hubspot-report.md     # /hubspot-report command
└── hubspot-ask.md        # /hubspot-ask command
```

---

## Quick Start (Standalone CLI)

```bash
# Install
pip install -e .

# Set your HubSpot Private App token
export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx

# Discover your schema
python -m hubspot_revops.cli schema

# Run reports
python -m hubspot_revops.cli report executive
python -m hubspot_revops.cli report pipeline --period Q1-2026
python -m hubspot_revops.cli report revenue --period 90d
python -m hubspot_revops.cli report funnel --period 6m
python -m hubspot_revops.cli report team --period Q1-2026

# Ask questions
python -m hubspot_revops.cli ask "Who are our top performing reps?"
python -m hubspot_revops.cli ask "What's the win rate trend?"
python -m hubspot_revops.cli ask "Show me pipeline by stage"
```

---

## Metrics Catalog

### Pipeline & Deal Metrics
| Metric | Description |
|---|---|
| Total Pipeline Value | Sum of all open deal amounts |
| Pipeline by Stage | Deals and value grouped by pipeline stage |
| Pipeline by Rep | Open pipeline per sales rep |
| Win Rate | Won deals / total closed deals (%) |
| Average Deal Size | Mean amount of closed-won deals |
| Sales Cycle Length | Average days from create to close for won deals |
| Pipeline Velocity | (deals x win rate x avg size) / avg cycle length |
| Stage Conversion | % of deals progressing between adjacent stages |
| Pipeline Coverage | Open pipeline / quota or target |
| Slip Rate | Deals that moved close date out / total deals |

### Revenue Metrics
| Metric | Description |
|---|---|
| Closed Revenue | Total closed-won amount in period |
| MRR / ARR | Monthly/annual recurring revenue |
| Revenue by Rep | Closed revenue grouped by deal owner |
| Revenue by Pipeline | Revenue broken down by pipeline |
| Revenue by Source | Revenue attributed to lead source |
| Expansion Revenue | Upsell/cross-sell revenue |
| Churn Revenue | Lost recurring revenue |
| Net Revenue Retention | (Starting MRR + expansion - churn) / Starting MRR |

### Funnel & Conversion Metrics
| Metric | Description |
|---|---|
| Lead → MQL | Contacts reaching marketing qualified stage |
| MQL → SQL | Marketing to sales qualified conversion |
| SQL → Opportunity | Sales qualified to deal creation |
| Opportunity → Customer | Deal to closed-won conversion |
| Full Funnel | End-to-end conversion rate |
| Lead Source Breakdown | Contacts grouped by original source |
| Time in Stage | Average duration at each lifecycle stage |

### Activity & Engagement Metrics
| Metric | Description |
|---|---|
| Activities per Deal | Engagements (calls, emails, meetings) per deal |
| Activity by Rep | Engagement volume grouped by owner |
| Calls / Emails / Meetings | Count by engagement type per period |
| Avg Response Time | Time between inbound and first reply |

### Team Performance
| Metric | Description |
|---|---|
| Rep Scorecard | Per-rep: pipeline, revenue, win rate, avg deal size |
| Quota Attainment | Won revenue vs. target |
| Win Rate by Rep | Individual win rates |
| Sales Cycle by Rep | Per-rep average cycle length |

### Forecast
| Metric | Description |
|---|---|
| Weighted Pipeline | Deal amounts weighted by stage probability |
| Forecast Categories | Breakdown by commit / best case / pipeline |

---

## HubSpot Setup

### Create a Private App

1. Go to **Settings → Integrations → Private Apps** in your HubSpot portal
2. Click **Create a private app**
3. Name it (e.g., "RevOps Analyst")
4. Under **Scopes**, select these **read-only** permissions:

| Scope | Purpose |
|---|---|
| `crm.objects.contacts.read` | Contact data and lifecycle stages |
| `crm.objects.companies.read` | Company data |
| `crm.objects.deals.read` | Deal pipeline and revenue data |
| `crm.objects.line_items.read` | Line item / product data |
| `crm.schemas.custom.read` | Custom object schemas |
| `crm.objects.owners.read` | Sales rep / owner data |
| `sales-email-read` | Email engagement data |

5. Click **Create app** and copy the access token
6. Set it: `export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx`

### Security Notes

- **Read-only** — this skill never creates, updates, or deletes CRM data
- Tokens are passed via environment variables only (never committed to code)
- Minimum-scope permissions — only what's needed for analytics
- Rate limit compliant: 100 req/10s (free), 190 req/10s (pro/enterprise), 5 req/s (search)

---

## Architecture

```
hubspot_revops/
├── client.py              # HubSpot API client — auth, rate limiting, retry
├── schema/
│   ├── discovery.py       # Auto-discover CRM objects, properties, pipelines
│   ├── models.py          # Pydantic models for schema elements
│   └── cache.py           # 24-hour schema cache to avoid API waste
├── extractors/
│   ├── base.py            # Base extractor with pagination & search
│   ├── deals.py           # Deal extraction with pipeline-aware filtering
│   ├── contacts.py        # Contact extraction with lifecycle filtering
│   ├── companies.py       # Company extraction
│   ├── activities.py      # Engagement extraction (calls, emails, meetings)
│   ├── pipelines.py       # Pipeline & stage metadata
│   ├── owners.py          # HubSpot owner/rep data
│   └── custom_objects.py  # Dynamic custom object extraction
├── metrics/
│   ├── pipeline.py        # Pipeline value, velocity, conversion, coverage
│   ├── revenue.py         # Closed revenue, MRR/ARR, by rep/pipeline
│   ├── conversion.py      # Funnel conversion rates across lifecycle
│   ├── activity.py        # Engagement metrics by type and owner
│   ├── team.py            # Rep scorecard and performance metrics
│   └── forecast.py        # Weighted pipeline and forecast categories
├── reports/
│   ├── generator.py       # Orchestrates extraction → metrics → formatting
│   ├── templates.py       # Markdown report templates
│   └── charts.py          # Optional matplotlib chart generation
├── nl_interface.py        # Natural language question → metric routing
└── cli.py                 # CLI entry point (schema / report / ask)
```

See [PLAN.md](PLAN.md) for the full implementation plan with API details, data flow, and phased roadmap.

---

## Requirements

- Python 3.10+
- HubSpot account with a Private App (any tier — Free, Starter, Pro, Enterprise)
- Dependencies: `hubspot-api-client`, `pandas`, `pydantic`, `matplotlib`, `rich`

---

## About FlashLabs.ai

<p align="center">
  <a href="https://flashlabs.ai">
    <img src="https://flashlabs.ai/images/flashclaw-logo.png" alt="FlashClaw" width="160"/>
  </a>
</p>

**[FlashLabs.ai](https://flashlabs.ai)** builds AI-powered GTM agents that help revenue teams sell smarter and faster. The HubSpot RevOps Analyst is one of many GTM skills available on the FlashLabs platform.

### FlashClaw — Enterprise OpenClaw

**[FlashClaw](https://flashlabs.ai)** is the enterprise-ready, hosted version of OpenClaw. It gives your GTM team production-grade AI agents without the hassle of self-hosting:

- **Hosted & Managed** — no infrastructure to maintain, always up to date
- **Enterprise Security** — SOC 2 compliant, SSO, role-based access, audit logs
- **Team Collaboration** — shared skills, shared context, team dashboards
- **Pre-Built GTM Skills** — RevOps, sales intelligence, pipeline management, and more
- **Secrets Management** — secure credential storage for API tokens
- **Priority Support** — dedicated onboarding and support for revenue teams

> **Ready to supercharge your GTM team with AI?**
>
> **[Start free with FlashClaw →](https://flashlabs.ai)**
>
> Or explore the [FlashLabs GTM Agent Gallery](https://flashlabs.ai) for more skills.

---

## Contributing

Contributions are welcome! See [PLAN.md](PLAN.md) for the implementation roadmap and open tasks.

## License

MIT
