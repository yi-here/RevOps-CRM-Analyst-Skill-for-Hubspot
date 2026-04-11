# RevOps CRM Analyst Skill for HubSpot

<p align="center">
  <img src="https://cdn.kyodonewsprwire.jp/prwfile/release/M108511/202603125506/_prw_PI2fl_sw68g5v6.png" alt="FlashClaw Logo" width="200"/>
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

1. **Connects** to HubSpot via OAuth 2.0 (browser-based, read-only, secure)
2. **Auto-discovers** your CRM schema — objects, properties, pipelines, associations
3. **Computes 40+ RevOps metrics** across pipeline, revenue, funnel, activity, and team
4. **Answers natural-language questions** — no SQL, no code, just ask
5. **Generates reports** — markdown tables, CSV exports, and charts

---

## Why Not Just Use HubSpot's MCP?

HubSpot ships an official MCP server at [`mcp.hubspot.com/anthropic`](https://developers.hubspot.com/mcp). It's genuinely great, and you should probably install it too:

```bash
claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic
```

But MCP alone is a **data access layer** — it gives the model tools to fetch records, then expects the model to compute the numbers inline. That works for exploratory questions. It doesn't work for CFO-grade RevOps reporting. Here's what this skill adds on top:

|  | HubSpot MCP (raw) | This skill |
|---|---|---|
| **What it is** | Data access layer — thin wrapper over HubSpot's REST API | Opinionated RevOps analyst — skill + 40+ pandas metric engines + CLI |
| **Numbers** | Model computes inline from raw records — drifts run-to-run | Deterministic pandas engines — byte-exact every run |
| **Scale** | Bounded by model context (~2k records practical limit) | Pages + aggregates server-side, handles 20k+ deals |
| **RevOps heuristics** | None — model re-derives from training each time | Baked in: win-rate n<5 filter, stage-age risk flags, velocity formula, fiscal-quarter parsing |
| **Report structure** | Varies per query — columns drift between runs | Locked templates — week-over-week comparison works |
| **Interpretation** | Optional, depends on how the user phrased the question | Mandatory: insights → risks → actions → drill-downs |
| **Write safety** | Exposes HubSpot's write endpoints — `crm_update_*`, `crm_delete_*` | Architecturally read-only — write code doesn't exist in the package |
| **Host reach** | Any MCP-compatible host (Claude Code, Cursor, Zed, Desktop) | Claude Code + Python CLI; automatically hands off unmatched questions to HubSpot's MCP via a `FALLBACK_TO_MCP` banner |
| **Best for** | Freeform queries, cross-tool composition, small portals where exact numbers don't matter | Canned RevOps reports, audit-grade precision, medium-large portals, scheduled "Monday pipeline review" workflows |

### They compose — and the skill already falls back to MCP today

This skill and HubSpot's MCP aren't competitors — they're layered. Users who want raw CRM exploration install the HubSpot MCP. Users who want analyst-grade reporting install this skill. Users who want both install both; they don't conflict.

**How the fallback works right now (not a future roadmap item):**

- **Primary path — raw queries.** The skill talks directly to HubSpot's REST API via `hubspot-api-client` (Search API with filters, paginated via `after` cursor). MCP is *not* in the hot path for canned reports. This is what makes the numbers deterministic and byte-exact — no model-in-the-loop between you and the aggregation.
- **Fallback path — hand off to MCP.** When `python -m hubspot_revops.cli ask "..."` hits a question that doesn't match any canned-metric keyword, or when a report method raises an exception (HubSpot 5xx after retry, unexpected schema shape, etc.), the skill does *not* silently route to the executive summary and it does *not* spew a Python traceback. It emits a structured `FALLBACK_TO_MCP` markdown banner — see `hubspot_revops/nl_interface.py:130` — that tells the calling agent: "I couldn't answer this deterministically; here's the original question, here are the HubSpot MCP tools to try (`search_deals`, `search_contacts`, `get_engagements`, …), and here's the install command if the MCP isn't present."

The net effect: **for the 40+ questions this skill was designed to answer, you get raw-query precision; for everything else, the skill knows it doesn't know, and the agent hands the question off to HubSpot's MCP for a raw-records lookup instead of making something up.** That's the "compose" story — not a wrapper, a graceful handoff.

### When to reach for which

**Pick HubSpot's MCP if you're asking things like:**
- *"Find all companies in California with MRR > $50k whose primary contact matches /VP.*Engineering/"*
- *"What's this specific deal's associated line items and their product categories?"*
- *"Compare HubSpot pipeline coverage with the forecast in Salesforce"* (cross-tool queries)

**Pick this skill if you're asking things like:**
- *"What's our pipeline by stage this quarter?"* (canned report, precise numbers, interpreted)
- *"Who are the top 5 reps by win rate, excluding reps with fewer than 5 closed deals?"* (RevOps heuristic baked in)
- *"Give me the executive summary I can show in Monday's leadership meeting"* (locked template, week-over-week comparable)
- *"My HubSpot portal has 18,000 open deals — what's total pipeline coverage?"* (beyond MCP's context window)
- *"I need the same pipeline report every Monday at 9am, and the numbers must match the board deck"* (deterministic, auditable)

**The short version:** MCP gives you a HubSpot data hose. This skill gives you a RevOps analyst. Different jobs.

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

Register a HubSpot public app at https://developers.hubspot.com (see
[HubSpot Setup](#hubspot-setup) below) and set the client ID and secret in
your OpenClaw/FlashClaw environment:

```bash
# OpenClaw
export HUBSPOT_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export HUBSPOT_CLIENT_SECRET=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# FlashClaw — use the secrets dashboard or:
flashclaw secret set HUBSPOT_CLIENT_ID xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
flashclaw secret set HUBSPOT_CLIENT_SECRET xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

On first run the skill opens a browser to complete HubSpot authorization;
tokens are cached under `~/.hubspot_revops/tokens.json` and refreshed
silently on subsequent runs. For fully headless CI, set
`HUBSPOT_ACCESS_TOKEN` instead and the OAuth flow is skipped.

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

The `SKILL.md` declares `HUBSPOT_CLIENT_ID` and `HUBSPOT_CLIENT_SECRET` as required env vars, `python3` as a required binary, and includes full instructions for the agent to discover schema, run reports, and interpret results.

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

# 3. Set your HubSpot OAuth credentials
#    (Register a public app at https://developers.hubspot.com — see HubSpot Setup below)
export HUBSPOT_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export HUBSPOT_CLIENT_SECRET=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# 4. Open Claude Code in this directory
#    (The first slash-command invocation will open a browser for HubSpot authorization.)
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

# (Recommended) Install HubSpot's official MCP server as the companion
# data-access layer. This skill handles canned reports; the MCP handles
# ad-hoc record lookups. They compose.
claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic

# Set your HubSpot OAuth app credentials
# (Register a public app at https://developers.hubspot.com — see HubSpot Setup below)
export HUBSPOT_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
export HUBSPOT_CLIENT_SECRET=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Discover your schema (first run opens a browser to authorize)
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

### Register a HubSpot OAuth App

The skill authenticates via OAuth 2.0 against a HubSpot public app that you
register once in your own HubSpot developer account. The app stays yours —
client ID and secret never leave your environment, and tokens are cached
locally under `~/.hubspot_revops/tokens.json` with `0600` permissions.

1. Go to https://developers.hubspot.com. If you do not already have a
   developer account, create one (free). Then open **Apps → Create app**.
2. **Auth tab:** set the redirect URL to `http://localhost:8976/callback`
   (or any free local port — set `HUBSPOT_REDIRECT_PORT` to match). Copy
   the **Client ID** and **Client Secret**.
3. **Scopes tab:** select these **read-only** permissions:

| Scope | Purpose |
|---|---|
| `oauth` | Required for all OAuth flows |
| `crm.objects.contacts.read` | Contact data and lifecycle stages |
| `crm.objects.companies.read` | Company data |
| `crm.objects.deals.read` | Deal pipeline and revenue data |
| `crm.objects.line_items.read` | Line item / product data |
| `crm.schemas.custom.read` | Custom object schemas |
| `crm.objects.owners.read` | Sales rep / owner data |
| `sales-email-read` | Email engagement data |

4. Install the app to a HubSpot account (the app's **Install URL** on the
   Auth tab — this connects the app to your CRM portal so it can be
   authorized by the OAuth flow).
5. Export the credentials:
   ```bash
   export HUBSPOT_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   export HUBSPOT_CLIENT_SECRET=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   ```
6. Run any CLI command — e.g. `python -m hubspot_revops.cli schema`. The
   first run opens your browser, you click **Connect app**, and HubSpot
   redirects back to `http://localhost:8976/callback`. The skill caches the
   refresh token and future runs complete silently.

### Advanced: Use a Static Token (CI / Headless)

If you cannot open a browser (CI runners, Docker containers, FlashClaw
secrets-manager deployments), set `HUBSPOT_ACCESS_TOKEN` to a Private App
token or a pre-minted OAuth access token. When this variable is set the
skill skips the interactive OAuth flow entirely.

```bash
export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx
python -m hubspot_revops.cli schema
```

### Security Notes

- **Read-only** — this skill never creates, updates, or deletes CRM data
- Credentials are passed via environment variables only (never committed to code)
- OAuth tokens are cached under `~/.hubspot_revops/tokens.json` with `0600` permissions
- Minimum-scope permissions — only what's needed for analytics
- Rate limit compliant: 100 req/10s (free), 190 req/10s (pro/enterprise), 5 req/s (search)

---

## Architecture

The skill is a layered, synchronous pipeline: **Auth → Client → Schema → Extractors → Metrics → Reports**. Each layer is independently testable, deterministic, and architecturally read-only.

```
┌──────────────┐   ┌──────────┐   ┌────────┐   ┌────────────┐   ┌─────────┐   ┌─────────┐
│ CLI / NL query│→ │ Schema   │→ │ Extract │→ │  Metrics    │→ │ Reports │→ │Markdown │
│ + TimeRange   │  │ (cached) │  │ (search │  │  (pandas    │  │(templates│  │  / CSV  │
└──────────────┘   └──────────┘   │  API)   │  │  groupby)   │  │  + chart)│  └─────────┘
                                   └────────┘   └────────────┘   └─────────┘
```

### Package Layout

```
hubspot_revops/
├── auth.py                # OAuth 2.0 flow with HMAC CSRF state, atomic token cache
│                          # (temp file + os.replace), refresh-token fallback to
│                          # interactive flow if rotated
├── client.py              # HubSpot SDK wrapper — token-bucket rate limiter
│                          # (100 req/10s general, 5 req/1s Search API), 5-attempt
│                          # exponential retry (1s → 16s) for 429/5xx
├── cli.py                 # CLI entry: schema / report / ask; fiscal-quarter,
│                          # calendar-month, and rolling-window period parsing
├── nl_interface.py        # Keyword-based question → report routing (no NLU dep)
│
├── schema/
│   ├── discovery.py       # Discovers objects, properties, pipelines, stages
│   │                      # (won/closed/probability), owners, associations
│   ├── models.py          # Pydantic models: CRMSchema, Pipeline, PipelineStage,
│   │                      # Owner, PropertySchema, ObjectSchema, AssociationDef
│   ├── cache.py           # 24h JSON schema cache (TTL via HUBSPOT_SCHEMA_CACHE_TTL)
│   └── stage_ids.py       # Pipeline ID resolution + stage label disambiguation
│                          # across multi-pipeline portals
│
├── extractors/
│   ├── base.py            # BaseExtractor with paginated search (after cursor),
│   │                      # TimeRange dataclass, batched associations (100/req)
│   ├── deals.py           # 27 deal properties incl. hs_acv/arr/mrr/tcv and
│   │                      # deal_currency_code; variant property sets per report
│   ├── contacts.py        # Lifecycle-stage date properties for funnel rates
│   ├── companies.py       # Company data
│   ├── activities.py      # calls / emails / meetings / notes / tasks with
│   │                      # per-type date property fallback chain
│   ├── pipelines.py       # Pipeline + stage metadata
│   ├── owners.py          # HubSpot owners / reps
│   └── custom_objects.py  # Dynamic custom-object extraction
│
├── metrics/
│   ├── _utils.py          # to_numeric_series / to_bool_series coercion guards
│   │                      # (crash-proof on empty DataFrames or missing columns)
│   ├── _quality.py        # Stale open deals + zero-engagement "ghost" deals
│   ├── pipeline.py        # Total value, by stage, win rate, avg deal size,
│   │                      # sales-cycle length, velocity formula
│   ├── revenue.py         # Multi-currency closed revenue, by owner, by pipeline,
│   │                      # MRR/ARR — never sums across currencies
│   ├── conversion.py      # Lead → MQL → SQL → Opp → Customer funnel with
│   │                      # graceful degradation on API errors
│   ├── activity.py        # Engagement summary + by-owner across 5 activity types
│   ├── team.py            # Per-rep, per-currency scorecard (one row per
│   │                      # owner × currency pair)
│   ├── forecast.py        # Stage-probability-weighted pipeline + category buckets
│   ├── forecast_bucket.py # Monthly Commit / Highly Likely / Best Case with
│   │                      # probability normalization (round to 2dp)
│   ├── closed_lost.py     # Per-rep lost scorecard, reason breakdown, ghost count,
│   │                      # lost-reason coverage warning (< 50% threshold)
│   └── meeting_history.py # Meetings-to-close, per-rep won vs. lost, top lost-deal
│                          # effort sinks
│
└── reports/
    ├── generator.py       # ReportGenerator — orchestrates extraction → metrics
    │                      # → template formatting; resolves --pipeline flag
    ├── templates.py       # Markdown templates with per-currency formatting
    │                      # (USD $, JPY ¥, EUR €, GBP £), period headers,
    │                      # snapshot-vs-period disclaimers
    └── charts.py          # Optional chart output
```

### Data Flow

```
cli.parse_time_range("Q1-2026")
    → TimeRange(start, end)  # microsecond precision at quarter end
         │
         ▼
ReportGenerator._resolve_pipeline(name_or_id)
    → stage_ids.resolve_pipeline_id()  # exact → case-insensitive → substring
         │
         ▼
Extractor.search_in_time_range(properties=[...])
    → paginated HubSpot Search API (after cursor, max 10k)
         │
         ▼
metrics/*.py  →  pandas DataFrame groupby / sum / mean
    (deterministic, alphabetical tiebreakers, multi-currency isolated)
         │
         ▼
reports/templates.format_*_report()
    → Markdown with per-currency formatting, insights, risks, actions
```

See [PLAN.md](PLAN.md) for the full implementation plan and phased roadmap.

---

## Engineering Highlights

These are the quiet correctness decisions that separate "code that runs" from "reports you can show the CFO." Each one exists because an earlier version got bitten — and each one is covered by tests in `tests/`.

### 1. Multi-Currency Isolation — Never Sum Across Currencies

HubSpot portals with deals in USD, JPY, EUR, and GBP quietly produce nonsense if you `sum(amount)` — ¥990,000 is not $990,000. Every money metric in this skill (`metrics/revenue.py`, `metrics/team.py`, `metrics/closed_lost.py`, `metrics/forecast_bucket.py`) buckets by `deal_currency_code` and returns a `by_currency: {USD: ..., JPY: ..., EUR: ...}` dict. A `primary_currency` field (highest deal count, alphabetical tiebreak) is exposed for back-compat callers that expect a single scalar. Team scorecards emit **one row per `(owner_id, currency)` pair** so rep performance is never cross-currency-smeared.

### 2. Stage Label Disambiguation for Multi-Pipeline Portals

Portals running "Sales" and "Japan" pipelines in parallel frequently have identical stage labels (`Qualified`, `Proposal`) in both. A naive `groupby(stage_label)` double-counts. `schema/stage_ids.py:get_pipeline_stage_labels()` tracks `(pipeline_id, stage_id)` tuples and renders `"Qualified (Sales)"` vs. `"Qualified (Japan)"` in reports. This is not standard HubSpot SDK behavior.

### 3. Consistent Won-Deal Filter

A real prior bug: the team scorecard applied `to_bool_series(hs_is_closed_won)` in Python, while revenue passed `won_only=True` to the HubSpot API filter. The two disagreed on how HubSpot serializes booleans, producing a ~$25K gap between team totals and company totals on one portal. `revenue._fetch_won()` now fetches closed deals once and applies the Python-side filter, so both code paths compute against identical data.

### 4. Probability Normalization for Forecast Bucketing

HubSpot returns stage probabilities as clean floats (`0.8`), noisy floats (`0.80000000000000004`), percentage integers (`80`), or already-normalized fractions. `forecast_bucket._normalize_probability()` detects the form and rounds to two decimals so a `>= 0.8` threshold reliably fires on "commit" stages. Before this, late-stage deals were being silently classified as Best Case.

### 5. Engagement Date Fallback

HubSpot's Search API doesn't consistently honour the same date filter across engagement types — `hs_meeting_start_time` works for meetings but not emails. `ActivityExtractor` tries the type-specific field first, falls back to `hs_lastmodifieddate`, then to an unfiltered search. This fixes the "activity report always shows 0" bug that made engagement metrics useless on some portals.

### 6. Crash-Proof Pandas Helpers

`metrics/_utils.py:to_numeric_series()` / `to_bool_series()` guarantee a valid `pd.Series` even when the DataFrame is empty or the column is missing. Without these, downstream `.sum()`, `.mean()`, `.fillna()` calls throw `AttributeError` on low-data portals. This was the root cause of the team-scorecard crash on sandbox accounts.

### 7. Quarter-End Microsecond Precision

`cli.parse_time_range("Q1-2026")` anchors the end date to `23:59:59.999999` on the final day of the quarter, not midnight. Otherwise deals closed between noon and midnight on 31 March silently drop from the Q1 report. Same care is taken for `month` / `last-month` / named months.

### 8. Ghost Deal & Data-Quality Detection

`metrics/_quality.py` surfaces two hygiene issues the HubSpot UI hides:

- **Stale open deals** — `closedate` in the past but `hs_is_closed = false`
- **Ghost deals** — closed-lost deals with zero associated meetings, calls, emails, or notes ("never actually worked")

`metrics/closed_lost.py` additionally warns when fewer than 50% of lost deals have a `closed_lost_reason` populated, because the reason breakdown is unreliable below that threshold.

### 9. Cross-Process Rate Limiter with Separate Search-API Bucket

`client.py` runs two independent token buckets: 100 req/10s for the general CRM API and 5 req/1s for the Search API (which has stricter limits HubSpot doesn't document loudly). Both buckets persist state in JSON files under `~/.hubspot_revops/` and wrap every read-modify-write in an `fcntl.flock` exclusive lock, so **multiple parallel Python processes coordinate through shared buckets** instead of each running an independent limiter that collectively blows past HubSpot's per-portal ceiling. An agent loop that launches nine reports in parallel now serializes their API calls safely instead of triggering 429 cascades.

Paired with a 5-attempt exponential-backoff retry (1s → 2s → 4s → 8s → 16s) for 429/5xx, the client survives the contacts-search 502 spikes that crash naive wrappers. On Windows (no `fcntl`) or any environment where `~/.hubspot_revops/` isn't writable, the skill falls back to the in-process limiter and emits a warning — parallel invocations in that fallback mode still need to run sequentially.

### 10. Atomic OAuth Token Cache

`auth.py` saves the refresh token via temp file + `os.replace()` so a process dying mid-write can never corrupt the cache. CSRF state tokens are HMAC-signed. If the refresh token is rotated out from under us, the client falls back to the interactive flow instead of erroring. Tokens live at `~/.hubspot_revops/tokens.json` with `0600` permissions.

### 11. Locked Schema, Deterministic Aggregation

Schema is discovered once per 24h and cached; every metric is computed against that snapshot. Pandas `groupby`/`sum` operations are deterministic. Tiebreakers (e.g. primary currency selection) are alphabetical. Reports generated from the same HubSpot snapshot are byte-exact across runs — critical for audit-grade RevOps reporting where week-over-week comparisons need stable columns.

### 12. Architecturally Read-Only

There are **no HubSpot write methods anywhere in the package**. Not in `client.py`, not in the extractors, not in the reports. The skill cannot create, update, or delete CRM records — even under prompt injection — because the code for doing so doesn't exist. This is a stronger guarantee than "the agent is configured to only read."

---

## Requirements

- Python 3.10+
- HubSpot developer account with a registered OAuth public app (any tier — Free, Starter, Pro, Enterprise)
- Dependencies: `hubspot-api-client`, `pandas`, `pydantic`, `matplotlib`, `rich`, `httpx`

---

## About FlashLabs.ai

<p align="center">
  <a href="https://flashlabs.ai">
    <img src="https://cdn.kyodonewsprwire.jp/prwfile/release/M108511/202603125506/_prw_PI2fl_sw68g5v6.png" alt="FlashClaw" width="160"/>
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
