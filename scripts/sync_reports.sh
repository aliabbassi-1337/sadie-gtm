#!/bin/bash
# Sync reports from S3 to OneDrive/SharePoint
#
# Usage:
#   ./scripts/sync_reports.sh         # Sync all states from S3 to OneDrive
#   ./scripts/sync_reports.sh TX      # Sync only Texas
#   ./scripts/sync_reports.sh FL TX   # Sync Florida and Texas

set -e

S3_BUCKET="sadie-gtm"
S3_BASE="s3://$S3_BUCKET/HotelLeadGen/USA"
REGION="eu-north-1"
ONEDRIVE_BASE="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-ValsoftCorporation/Sadie Shared - Sadie Lead Gen/USA"

# If specific states provided, use those; otherwise sync all
if [ $# -gt 0 ]; then
    STATES="$@"
else
    # Get all state directories from S3
    STATES=$(aws s3 ls "$S3_BASE/" --region $REGION | awk '{print $2}' | tr -d '/')
fi

echo "=== Syncing from S3 to OneDrive ==="
echo "States: $STATES"
echo ""

for STATE in $STATES; do
    echo "--- Syncing $STATE ---"
    mkdir -p "$ONEDRIVE_BASE/$STATE"
    aws s3 sync "$S3_BASE/$STATE" "$ONEDRIVE_BASE/$STATE" --region $REGION
    echo ""
done

echo "Done! Files synced to: $ONEDRIVE_BASE"
