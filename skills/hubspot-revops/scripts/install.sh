#!/usr/bin/env bash
# Install script for the HubSpot RevOps skill
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "Installing HubSpot RevOps CRM Analyst Skill..."

# Check Python version
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 is required but not found."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Python version: $PYTHON_VERSION"

# Install the package
cd "$PROJECT_DIR"
pip install -e . 2>&1

# Verify installation
python3 -c "import hubspot_revops; print(f'hubspot-revops v{hubspot_revops.__version__} installed successfully')"

# Check for token
if [ -z "$HUBSPOT_ACCESS_TOKEN" ]; then
    echo ""
    echo "WARNING: HUBSPOT_ACCESS_TOKEN is not set."
    echo "Set it with: export HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx"
    echo "Create a Private App at: Settings → Integrations → Private Apps"
    echo ""
    echo "Required scopes:"
    echo "  - crm.objects.contacts.read"
    echo "  - crm.objects.companies.read"
    echo "  - crm.objects.deals.read"
    echo "  - crm.objects.line_items.read"
    echo "  - crm.schemas.custom.read"
    echo "  - crm.objects.owners.read"
    echo "  - sales-email-read"
else
    echo "HUBSPOT_ACCESS_TOKEN is set."
fi

echo ""
echo "Done! Try: python -m hubspot_revops.cli schema"
