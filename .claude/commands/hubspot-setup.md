---
name: hubspot-setup
description: One-shot setup for HubSpot RevOps analysis — installs HubSpot's official MCP server as the companion data-access layer and warms this skill's 24h schema cache so pipeline IDs, owners, and currency-by-pipeline are immediately available. Run this once when starting work on a HubSpot portal.
allowed-tools: Bash(claude *) Bash(python *) Bash(pip *) Read
---

# HubSpot RevOps Setup

Wire up both the companion MCP and this skill's schema cache in one pass.

## Steps

1. **Check credentials.** Ensure `HUBSPOT_CLIENT_ID` and `HUBSPOT_CLIENT_SECRET` are set (from a HubSpot public app registered at https://developers.hubspot.com). For CI/headless runs, `HUBSPOT_ACCESS_TOKEN` works instead. If none are set, ask the user to provide them before continuing.

2. **Install HubSpot's official MCP server** if not already registered. This exposes raw record-lookup tools (`search_deals`, `get_deal`, `search_contacts`, `get_company`, `get_engagements`, etc.) that compose with this skill's canned reports:

```bash
claude mcp list | grep -q '^hubspot\b' || \
  claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic
```

3. **Ensure the skill is installed locally**:

```bash
cd $PROJECT_DIR && pip install -e . 2>/dev/null
```

4. **Warm the 24h schema cache** and print the context block — this gives you pipeline IDs, stage IDs, owner names, and object counts so every follow-up query can write precise filters without rediscovering:

```bash
cd $PROJECT_DIR && python -m hubspot_revops.cli schema
```

5. **Summarize** what was set up:
   - Whether HubSpot's MCP was newly installed or already present
   - The pipelines discovered (label + pipeline_id + stage count)
   - The owner count
   - A reminder of the division of labor:
     - **HubSpot MCP** → ad-hoc record lookups, filtered searches, single-deal details
     - **This skill's CLI** → deterministic RevOps reports (`python -m hubspot_revops.cli report <type> [--pipeline <name>] [--period <period>]`)

After this command runs once, subsequent questions in the session can jump straight to either MCP tool calls or `python -m hubspot_revops.cli report ...` without re-running setup — the schema cache is good for 24 hours.
