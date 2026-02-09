#!/bin/bash
# Sync reports from S3 to OneDrive/SharePoint
#
# Usage:
#   ./scripts/sync_reports.sh         # Sync all states + reports from S3 to OneDrive
#   ./scripts/sync_reports.sh "Texas" # Sync only Texas
#   ./scripts/sync_reports.sh "Florida" "New York"   # Sync Florida and New York

set -e

S3_BUCKET="sadie-gtm"
S3_BASE="s3://$S3_BUCKET/HotelLeadGen"
ONEDRIVE_BASE="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-ValsoftCorporation/Sadie Shared - Sadie Lead Gen"
AWS_REGION="eu-north-1"

echo "=== Syncing from S3 to OneDrive ==="

# Countries to sync
COUNTRIES=("United States" "Australia" "Canada" "United Kingdom")

# If specific states provided, use those; otherwise sync all countries
if [ $# -gt 0 ]; then
    # Sync provided states (assumes United States)
    for STATE in "$@"; do
        mkdir -p "$ONEDRIVE_BASE/United States/$STATE"
        echo "Syncing: $STATE"
        aws s3 sync "$S3_BASE/United States/$STATE/" "$ONEDRIVE_BASE/United States/$STATE/" --region "$AWS_REGION"
    done
else
    # Sync all countries
    for COUNTRY in "${COUNTRIES[@]}"; do
        echo "Syncing all $COUNTRY states..."
        mkdir -p "$ONEDRIVE_BASE/$COUNTRY"
        aws s3 sync "$S3_BASE/$COUNTRY/" "$ONEDRIVE_BASE/$COUNTRY/" --region "$AWS_REGION"
    done
fi

# Sync booking-engines directory (Cloudbeds, RMS, SiteMinder, Mews)
mkdir -p "$ONEDRIVE_BASE/booking-engines"
echo "Syncing: booking-engines"
aws s3 sync "$S3_BASE/booking-engines/" "$ONEDRIVE_BASE/booking-engines/" --region "$AWS_REGION"

echo ""
echo "Done! Files synced to: $ONEDRIVE_BASE"
