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

# Check for OAuth credentials (or the CI escape-hatch static token)
if [ -n "$HUBSPOT_ACCESS_TOKEN" ]; then
    echo "HUBSPOT_ACCESS_TOKEN is set (CI / headless mode — OAuth flow will be skipped)."
elif [ -n "$HUBSPOT_CLIENT_ID" ] && [ -n "$HUBSPOT_CLIENT_SECRET" ]; then
    echo "HubSpot OAuth credentials are set. The first run will open a browser to authorize access."
else
    echo ""
    echo "WARNING: HubSpot credentials are not set."
    echo "Register a HubSpot public app at: https://developers.hubspot.com"
    echo "Then export the client ID and secret:"
    echo "  export HUBSPOT_CLIENT_ID=..."
    echo "  export HUBSPOT_CLIENT_SECRET=..."
    echo ""
    echo "Configure the app's redirect URL to: http://localhost:8976/callback"
    echo "(Override with HUBSPOT_REDIRECT_PORT if 8976 is unavailable.)"
    echo ""
    echo "Required scopes on the app:"
    echo "  - crm.objects.contacts.read"
    echo "  - crm.objects.companies.read"
    echo "  - crm.objects.deals.read"
    echo "  - crm.objects.line_items.read"
    echo "  - crm.schemas.custom.read"
    echo "  - crm.objects.owners.read"
    echo "  - sales-email-read"
    echo "  - oauth"
    echo ""
    echo "(For CI/headless runs, set HUBSPOT_ACCESS_TOKEN instead to skip the OAuth flow.)"
fi

echo ""
echo "Done! Try: python -m hubspot_revops.cli schema"
