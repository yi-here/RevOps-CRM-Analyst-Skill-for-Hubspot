---
name: hubspot-schema
description: Discover and display the HubSpot CRM schema — objects, properties, pipelines, associations, owners. Use when the user wants to understand their CRM structure.
allowed-tools: Bash(python *) Read
---

# Discover HubSpot CRM Schema

Connect to HubSpot and discover the full CRM schema.

## Steps

1. Ensure `HUBSPOT_ACCESS_TOKEN` is set in the environment. If not, ask the user for it.
2. Run the schema discovery:

```bash
cd $PROJECT_DIR && python -m hubspot_revops.cli schema --refresh
```

3. Present the results showing:
   - All CRM objects (standard + custom) with property counts
   - Deal/ticket pipelines with stage names and probabilities
   - Association definitions between objects
   - Owner/rep list

If dependencies are not installed, first run:
```bash
cd $PROJECT_DIR && pip install -e .
```
