---
name: hubspot-ask
description: Answer natural-language business questions about HubSpot CRM data. Use when the user asks questions like "What's our win rate?", "Who are the top reps?", "Show me pipeline by stage", "How's revenue trending?", or any RevOps/sales/marketing metric question.
argument-hint: [your question about business metrics]
allowed-tools: Bash(python *) Read
---

# Answer HubSpot Business Question

Answer a natural-language RevOps question using live HubSpot CRM data.

**Question:** $ARGUMENTS

## Steps

1. Ensure `HUBSPOT_CLIENT_ID` and `HUBSPOT_CLIENT_SECRET` are set (from a HubSpot public app registered at https://developers.hubspot.com). The first run will open a browser to complete authorization; subsequent runs use the cached refresh token. If the variables are missing, ask the user. For CI/headless, `HUBSPOT_ACCESS_TOKEN` can be set instead to skip the OAuth flow.
2. Run the question through the NL interface:

```bash
cd $PROJECT_DIR && python -m hubspot_revops.cli ask "$ARGUMENTS"
```

3. Present the results clearly.
4. **If the output contains `FALLBACK_TO_MCP`**, the skill could not
   confidently answer the question. Do NOT re-run the same CLI command.
   Instead, hand off to HubSpot's official MCP server:
   - Parse the original question from the banner
   - Pick the right MCP tool: `search_deals` / `search_contacts` /
     `search_companies` with filters, `get_deal` / `get_contact` /
     `get_company` for single-record lookups, or `get_engagements`
     for activity timelines
   - Run the raw query and synthesize the answer from the records
   - If HubSpot's MCP is not installed, tell the user to run
     `claude mcp add --transport http hubspot https://mcp.hubspot.com/anthropic`
     or suggest a related canned report

5. If the question is clearly ambiguous (no fallback banner, but the
   auto-classified report doesn't feel right), interpret it using
   these guidelines:
   - Pipeline questions → pipeline report
   - Revenue/bookings/MRR → revenue report
   - Win rate/conversion/funnel → funnel or pipeline metrics
   - Rep/team/quota → team scorecard
   - Calls/emails/meetings → activity report
   - Forecast/weighted → forecast metrics
   - General "how are we doing" → executive summary

6. Offer to drill deeper into specific metrics or compare time periods.

If dependencies are not installed, first run:
```bash
cd $PROJECT_DIR && pip install -e .
```
