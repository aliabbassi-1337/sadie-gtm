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

# If specific states provided, use those; otherwise sync all
if [ $# -gt 0 ]; then
    # Sync provided states
    for STATE in "$@"; do
        mkdir -p "$ONEDRIVE_BASE/USA/$STATE"
        echo "Syncing: $STATE"
        aws s3 sync "$S3_BASE/USA/$STATE/" "$ONEDRIVE_BASE/USA/$STATE/" --region "$AWS_REGION"
    done
else
    # Sync all state directories from S3 using aws s3 sync (handles spaces properly)
    echo "Syncing all USA states..."
    aws s3 sync "$S3_BASE/USA/" "$ONEDRIVE_BASE/USA/" --region "$AWS_REGION"
fi

# Sync reports directory
mkdir -p "$ONEDRIVE_BASE/reports"
echo "Syncing: reports"
aws s3 sync "$S3_BASE/reports/" "$ONEDRIVE_BASE/reports/" --region "$AWS_REGION"

# Sync crawl-data directory (Cloudbeds, RMS, IPMS247, etc.)
mkdir -p "$ONEDRIVE_BASE/crawl-data"
echo "Syncing: crawl-data"
aws s3 sync "$S3_BASE/crawl-data/" "$ONEDRIVE_BASE/crawl-data/" --region "$AWS_REGION"

echo ""
echo "Done! Files synced to: $ONEDRIVE_BASE"
