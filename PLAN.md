# RevOps CRM Analyst Skill for HubSpot — Implementation Plan

## Overview

A Python-based skill that connects to HubSpot's CRM API, auto-discovers the
customer's schema, computes RevOps metrics, and answers natural-language
business questions. Designed to run inside Claude Code, OpenClaw, or any
LLM-powered coding agent.

---

## 1. Architecture

```
hubspot_revops/
├── __init__.py
├── client.py                 # HubSpot API client (auth, rate-limiting, pagination)
├── schema/
│   ├── __init__.py
│   ├── discovery.py          # Introspect CRM objects, properties, pipelines
│   ├── models.py             # Pydantic models for CRM objects
│   └── cache.py              # Local schema cache (avoid repeated API calls)
├── extractors/
│   ├── __init__.py
│   ├── base.py               # Base extractor with pagination + rate-limit handling
│   ├── deals.py              # Deals (+ line items, quotes)
│   ├── contacts.py           # Contacts
│   ├── companies.py          # Companies
│   ├── activities.py         # Engagements: calls, emails, meetings, notes, tasks
│   ├── pipelines.py          # Pipeline definitions & stage metadata
│   ├── owners.py             # HubSpot users / owners
│   └── custom_objects.py     # Dynamically-discovered custom objects
├── metrics/
│   ├── __init__.py
│   ├── pipeline.py           # Pipeline value, velocity, stage conversion
│   ├── revenue.py            # MRR, ARR, expansion, churn, net retention
│   ├── conversion.py         # Lead→MQL→SQL→Opp→Customer funnel
│   ├── activity.py           # Activity counts, response time, engagement
│   ├── team.py               # Rep-level performance, quota attainment
│   └── forecast.py           # Weighted pipeline, commit vs. best-case
├── reports/
│   ├── __init__.py
│   ├── generator.py          # Orchestrates metric computation → report
│   ├── templates.py          # Markdown report templates
│   └── charts.py             # matplotlib/plotly chart generation (optional)
├── nl_interface.py           # Natural-language question → metric mapping
└── cli.py                    # CLI entry point for standalone use

tests/
├── __init__.py
├── conftest.py               # Shared fixtures, mock API responses
├── test_client.py
├── test_schema_discovery.py
├── test_extractors.py
├── test_metrics.py
└── fixtures/                 # JSON fixtures mirroring HubSpot API responses
    ├── contacts.json
    ├── deals.json
    ├── pipelines.json
    ├── properties_deals.json
    └── schemas.json
```

---

## 2. HubSpot API Integration Layer

### 2.1 Authentication

| Method | Use Case |
|---|---|
| **Private App Token** | Single-account (primary path for this skill) |
| **OAuth 2.0** | Multi-account / marketplace apps (future) |

- API keys are **deprecated** — use Bearer tokens only.
- Token provided via `HUBSPOT_ACCESS_TOKEN` env var or passed at init.
- SDK: `hubspot-api-client` (`pip install hubspot-api-client`).

```python
from hubspot import HubSpot
client = HubSpot(access_token=os.environ["HUBSPOT_ACCESS_TOKEN"])
```

### 2.2 API Versioning

HubSpot is transitioning to **date-based versioning**:
- Legacy: `/crm/v3/objects/deals`
- New (2026+): `/crm/objects/2026-03/deals`

The Python SDK abstracts this. We'll use the SDK's high-level methods and
fall back to raw HTTP only for endpoints the SDK doesn't cover.

### 2.3 Rate Limiting & Pagination

| Tier | Limit |
|---|---|
| Free / Starter | 100 requests / 10 seconds |
| Professional / Enterprise | 190 requests / 10 seconds |

- **Pagination**: Cursor-based (`after` param). The SDK's `get_all()` handles this.
- **Search API**: Max 10,000 results per query. Use date-range windowing for larger sets.
- **Retry strategy**: Exponential backoff on 429 responses (built into SDK).

### 2.4 Key API Endpoints

| Capability | Endpoint | SDK Method |
|---|---|---|
| List objects | `GET /crm/v3/objects/{type}` | `crm.{type}.basic_api.get_page()` |
| Get object | `GET /crm/v3/objects/{type}/{id}` | `crm.{type}.basic_api.get_by_id()` |
| Search objects | `POST /crm/v3/objects/{type}/search` | `crm.{type}.search_api.do_search()` |
| Object properties | `GET /crm/v3/properties/{type}` | `crm.properties.core_api.get_all()` |
| Custom obj schemas | `GET /crm/v3/schemas` | `crm.schemas.core_api.get_all()` |
| Pipelines | `GET /crm/v3/pipelines/{type}` | `crm.pipelines.pipelines_api.get_all()` |
| Pipeline stages | `GET /crm/v3/pipelines/{type}/{id}/stages` | `crm.pipelines.pipeline_stages_api.get_all()` |
| Associations | `GET /crm/v4/associations/{from}/{to}/batch/read` | `crm.associations.v4.batch_api` |
| Owners | `GET /crm/v3/owners` | `crm.owners.owners_api.get_page()` |
| Engagements | `GET /crm/v3/objects/engagements` | via objects API |

### 2.5 Search API Filter Operators

```
EQ, NEQ, LT, LTE, GT, GTE, BETWEEN, IN, NOT_IN,
HAS_PROPERTY, NOT_HAS_PROPERTY, CONTAINS_TOKEN, NOT_CONTAINS_TOKEN
```

- Max **5 filter groups** (OR logic between groups)
- Max **6 filters per group** (AND logic within group)
- Max **18 filters total**

---

## 3. Schema Discovery & Understanding

### Phase 1: Auto-Discovery (runs on first connect)

1. **Enumerate standard objects**: Contacts, Companies, Deals, Tickets, Line Items, Products, Quotes
2. **Discover custom objects**: `GET /crm/v3/schemas` → list all custom object definitions
3. **For each object, fetch properties**: `GET /crm/v3/properties/{objectType}`
   - Property name, label, type, field type, options (for enums)
   - Identify calculated properties vs. user-defined
4. **Fetch pipelines & stages**: For Deals and Tickets (and any custom pipelined objects)
   - Stage names, display order, probability (for deals)
5. **Fetch association definitions**: Which objects link to which
6. **Fetch owners**: Map owner IDs → names/emails/teams

### Phase 2: Schema Model

Build a `CRMSchema` object that holds:

```python
@dataclass
class CRMSchema:
    objects: dict[str, ObjectSchema]        # name → schema
    pipelines: dict[str, list[Pipeline]]    # objectType → pipelines
    associations: list[AssociationDef]       # from/to/type
    owners: dict[str, Owner]                # ownerId → Owner
    custom_properties: dict[str, list[Property]]  # objectType → custom props
    generated_at: datetime
```

### Phase 3: Cache

- Cache schema to `.hubspot_schema_cache.json` (TTL: 24 hours)
- Force refresh via `--refresh-schema` flag
- On startup: load cache if fresh, else re-discover

### Phase 4: Schema Summary for LLM

Generate a human-readable schema summary that the LLM can use to understand
what data is available:

```
CRM Schema Summary:
- Deals: 45 properties (amount, dealstage, closedate, pipeline, hubspot_owner_id, ...)
  Pipelines: Sales Pipeline (7 stages), Enterprise Pipeline (5 stages)
- Contacts: 62 properties (email, firstname, lastname, lifecyclestage, hs_lead_status, ...)
- Companies: 38 properties (name, domain, industry, annualrevenue, ...)
- Custom Objects: Subscriptions (12 properties), Projects (8 properties)
- Associations: Deal↔Contact, Deal↔Company, Contact↔Company, ...
```

---

## 4. RevOps Metrics Catalog

### 4.1 Pipeline & Deal Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Total Pipeline Value** | Σ(deal.amount) where dealstage ∉ closed | Deals |
| **Pipeline by Stage** | Group deals by stage, sum amounts | Deals + Pipelines |
| **Pipeline by Rep** | Group deals by owner, sum amounts | Deals + Owners |
| **Win Rate** | Won deals / (Won + Lost deals) over period | Deals |
| **Average Deal Size** | Σ(won deal amounts) / count(won deals) | Deals |
| **Sales Cycle Length** | Avg(closedate - createdate) for won deals | Deals |
| **Pipeline Velocity** | (# deals × win rate × avg deal size) / avg cycle length | Computed |
| **Stage Conversion Rate** | Deals entering stage N+1 / Deals entering stage N | Deal history |
| **Pipeline Coverage** | Open pipeline / quota or target | Deals + config |
| **Deals Created** | Count of deals created in period | Deals |
| **Deals Closed Won/Lost** | Count by outcome in period | Deals |
| **Slip Rate** | Deals that moved close date out / total deals | Deals |

### 4.2 Revenue Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Closed Revenue** | Σ(amount) for won deals in period | Deals |
| **MRR / ARR** | From recurring revenue properties or line items | Line Items / Custom |
| **Expansion Revenue** | Revenue from upsells/cross-sells | Deals tagged as expansion |
| **Churn Revenue** | Lost recurring revenue in period | Deals / Custom |
| **Net Revenue Retention** | (Starting MRR + expansion - churn) / Starting MRR | Computed |
| **Revenue by Source** | Group won deals by lead source | Deals + Contacts |
| **Revenue by Product** | Group won deals by line items / products | Deals + Line Items |

### 4.3 Funnel & Conversion Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Lead → MQL** | Contacts reaching MQL stage / new contacts | Contacts |
| **MQL → SQL** | Contacts reaching SQL / MQL contacts | Contacts |
| **SQL → Opportunity** | Deals created / SQL contacts | Contacts + Deals |
| **Opportunity → Customer** | Won deals / total opportunities | Deals |
| **Full Funnel Conversion** | Customers / Total leads | Computed |
| **Lead Source Breakdown** | Contacts by original source | Contacts |
| **Time in Stage** | Avg time contacts spend in each lifecycle stage | Contacts |

### 4.4 Activity & Engagement Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Activities per Deal** | Engagements associated to deal / deal count | Engagements + Deals |
| **Emails Sent/Received** | Count by type per period | Engagements |
| **Calls Made** | Count of call engagements | Engagements |
| **Meetings Booked** | Count of meeting engagements | Engagements |
| **Avg Response Time** | Time between inbound email and first reply | Engagements |
| **Engagement Score** | Composite of activities, recency, frequency | Computed |

### 4.5 Team Performance Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Pipeline per Rep** | Open pipeline grouped by owner | Deals + Owners |
| **Quota Attainment** | Won revenue / quota (quota from config) | Deals + Config |
| **Win Rate by Rep** | Per-rep win rate | Deals + Owners |
| **Avg Deal Size by Rep** | Per-rep average | Deals + Owners |
| **Activity Volume by Rep** | Engagements grouped by owner | Engagements + Owners |
| **Sales Cycle by Rep** | Per-rep avg cycle length | Deals + Owners |

### 4.6 Forecast Metrics

| Metric | Formula | Data Source |
|---|---|---|
| **Weighted Pipeline** | Σ(deal.amount × stage.probability) | Deals + Pipelines |
| **Forecast by Category** | Group by forecast category property | Deals |
| **Best Case / Commit / Closed** | Standard forecast buckets | Deals |
| **Historical Accuracy** | Prior forecast vs. actual closed | Historical snapshots |

---

## 5. Data Retrieval Strategy

### 5.1 Efficient Data Fetching

```
┌─────────────────────────────────────────────┐
│             User Question                   │
│  "What's our win rate by rep this quarter?" │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│         NL Intent Classifier                │
│  → metrics needed: win_rate, by_rep         │
│  → time range: current quarter              │
│  → objects needed: deals, owners            │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│         Smart Extractor                     │
│  1. Search deals closed this quarter        │
│  2. Fetch owner details                     │
│  3. Group by owner, compute win rate        │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│         Report Generator                    │
│  → Markdown table / chart / summary         │
└─────────────────────────────────────────────┘
```

### 5.2 Search-First Approach

Rather than dumping all objects, use the **CRM Search API** to fetch only
what's needed:

```python
# Example: Deals closed-won this quarter
search_request = PublicObjectSearchRequest(
    filter_groups=[{
        "filters": [
            {"propertyName": "dealstage", "operator": "EQ", "value": "closedwon"},
            {"propertyName": "closedate", "operator": "GTE", "value": quarter_start_ms},
            {"propertyName": "closedate", "operator": "LTE", "value": quarter_end_ms},
        ]
    }],
    properties=["amount", "dealstage", "closedate", "hubspot_owner_id", "pipeline"],
    limit=100
)
```

### 5.3 Association-Enriched Queries

For metrics that span objects (e.g., revenue by lead source):
1. Fetch deals via search
2. Batch-fetch associated contacts via Associations API
3. Enrich deal data with contact properties (lead source)
4. Compute metric

### 5.4 Caching & Performance

- **Schema cache**: 24h TTL, stored as JSON
- **Query result cache**: In-memory, 5-minute TTL (for repeated questions)
- **Incremental fetching**: Use `lastmodifieddate` filters for updates
- **Batch operations**: Use batch read endpoints (up to 100 IDs per call)

---

## 6. Report & Dashboard Generation

### 6.1 Output Formats

| Format | Use Case |
|---|---|
| **Markdown tables** | Primary — works in Claude Code, terminals, PRs |
| **CSV export** | For spreadsheet analysis |
| **JSON** | For programmatic consumption |
| **matplotlib/plotly charts** | Saved as PNG/HTML for visual dashboards |

### 6.2 Pre-Built Report Templates

1. **Executive Summary**: Pipeline value, win rate, revenue, forecast — one page
2. **Pipeline Review**: Stage-by-stage breakdown, velocity, coverage
3. **Rep Scorecard**: Per-rep metrics with rankings
4. **Funnel Analysis**: Full funnel with conversion rates and bottlenecks
5. **Revenue Report**: Closed revenue, MRR trends, expansion/churn
6. **Activity Report**: Team engagement metrics, response times
7. **Forecast Report**: Weighted pipeline, commit vs. best case

### 6.3 Natural Language Queries (Examples)

The skill should handle questions like:

- "What's our pipeline looking like?"
- "Who are the top performing reps this quarter?"
- "What's the win rate trend over the last 6 months?"
- "Show me deals stuck in negotiation for more than 30 days"
- "What's our average sales cycle by deal size?"
- "How many MQLs converted to SQLs last month?"
- "What's our revenue forecast for this quarter?"
- "Which lead sources are driving the most revenue?"
- "Show me the full funnel conversion rates"
- "What's our churn rate trending like?"
- "Compare Q1 vs Q2 pipeline performance"
- "Which deals are at risk of slipping?"

---

## 7. Implementation Phases

### Phase 1: Foundation (Core Connection & Schema)
- [ ] Project setup (pyproject.toml, dependencies, .gitignore, .env.example)
- [ ] HubSpot client wrapper with auth, rate limiting, retry
- [ ] Schema discovery: enumerate objects, properties, pipelines, associations
- [ ] Schema caching with TTL
- [ ] Schema summary generation for LLM context
- [ ] Basic tests with mocked API responses

### Phase 2: Data Extraction
- [ ] Base extractor with pagination and search support
- [ ] Deal extractor (with line items)
- [ ] Contact extractor
- [ ] Company extractor
- [ ] Activity/engagement extractor
- [ ] Pipeline & stage extractor
- [ ] Owner extractor
- [ ] Custom object extractor
- [ ] Association resolver (cross-object enrichment)

### Phase 3: Metrics Engine
- [ ] Pipeline metrics (value, velocity, conversion, coverage)
- [ ] Revenue metrics (closed, MRR/ARR, expansion, churn, NRR)
- [ ] Funnel/conversion metrics (lead → customer)
- [ ] Activity metrics (per deal, per rep, response times)
- [ ] Team performance metrics (per rep breakdowns)
- [ ] Forecast metrics (weighted pipeline, categories)
- [ ] Time-series support (compare periods, trends)

### Phase 4: Reporting & Interface
- [ ] Markdown report generator
- [ ] Pre-built report templates (exec summary, pipeline review, etc.)
- [ ] CSV/JSON export
- [ ] Chart generation (matplotlib)
- [ ] Natural language query classifier
- [ ] CLI entry point

### Phase 5: Skill Integration
- [ ] CLAUDE.md with skill instructions and usage examples
- [ ] Integration with Claude Code as a skill
- [ ] OpenClaw / other agent compatibility
- [ ] Error handling for common HubSpot API issues
- [ ] Documentation and examples

---

## 8. Dependencies

```
hubspot-api-client>=9.0.0    # Official HubSpot Python SDK
pydantic>=2.0                # Data validation & schema models
pandas>=2.0                  # Data manipulation & aggregation
matplotlib>=3.7              # Chart generation
python-dotenv>=1.0           # Environment variable management
rich>=13.0                   # Terminal formatting & tables
httpx>=0.27                  # HTTP client (fallback for SDK gaps)
pytest>=8.0                  # Testing
pytest-asyncio>=0.23         # Async test support
```

---

## 9. Configuration

```env
# .env
HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx    # Private app token
HUBSPOT_PORTAL_ID=12345678               # Optional: portal ID

# Optional overrides
HUBSPOT_RATE_LIMIT=100                   # Requests per 10s
HUBSPOT_SCHEMA_CACHE_TTL=86400          # Schema cache TTL in seconds
HUBSPOT_QUERY_CACHE_TTL=300             # Query result cache TTL in seconds
```

---

## 10. Example Skill Usage (Claude Code)

```
User: Connect to my HubSpot and show me pipeline health

Skill:
1. Reads HUBSPOT_ACCESS_TOKEN from environment
2. Discovers schema (or loads cache)
3. Fetches open deals, pipelines, stages
4. Computes: total pipeline, by stage, velocity, coverage
5. Returns:

## Pipeline Health Report — Q2 2026

| Stage           | # Deals | Value        | Avg Age (days) |
|-----------------|---------|--------------|----------------|
| Qualified       | 23      | $1,245,000   | 12             |
| Demo Scheduled  | 15      | $890,000     | 18             |
| Proposal Sent   | 8       | $520,000     | 25             |
| Negotiation     | 5       | $380,000     | 34             |
| **Total Open**  | **51**  | **$3,035,000** |              |

**Key Metrics:**
- Win Rate (90d): 32%
- Avg Deal Size: $62,000
- Avg Sales Cycle: 45 days
- Pipeline Velocity: $1.34M/month
- Pipeline Coverage: 2.4x (vs. $1.25M target)

⚠️ 3 deals in Negotiation are >30 days old — review recommended.
```

---

## 11. Security Considerations

- **Never log or display access tokens**
- **Never commit .env files** (use .gitignore)
- **Scope private app to minimum required permissions:**
  - `crm.objects.contacts.read`
  - `crm.objects.companies.read`
  - `crm.objects.deals.read`
  - `crm.objects.line_items.read`
  - `crm.schemas.custom.read`
  - `crm.objects.owners.read`
  - `sales-email-read`
- **Read-only** — this skill never creates, updates, or deletes CRM data
- **Rate limit compliance** — respect HubSpot's limits to avoid account throttling
