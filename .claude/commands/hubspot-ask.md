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

1. Ensure `HUBSPOT_ACCESS_TOKEN` is set. If not, ask the user.
2. Run the question through the NL interface:

```bash
cd $PROJECT_DIR && python -m hubspot_revops.cli ask "$ARGUMENTS"
```

3. Present the results clearly.
4. If the question is ambiguous, interpret it using these guidelines:
   - Pipeline questions → pipeline report
   - Revenue/bookings/MRR → revenue report  
   - Win rate/conversion/funnel → funnel or pipeline metrics
   - Rep/team/quota → team scorecard
   - Calls/emails/meetings → activity report
   - Forecast/weighted → forecast metrics
   - General "how are we doing" → executive summary

5. Offer to drill deeper into specific metrics or compare time periods.

If dependencies are not installed, first run:
```bash
cd $PROJECT_DIR && pip install -e .
```
