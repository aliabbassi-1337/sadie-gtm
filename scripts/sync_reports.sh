#!/bin/bash
# Sync reports from S3 to OneDrive/SharePoint
#
# Usage:
#   ./scripts/sync_reports.sh    # Sync all reports from S3 to OneDrive

set -e

S3_BUCKET="sadie-gtm"
S3_EXPORTS="s3://$S3_BUCKET/HotelLeadGen/USA/FL"
REGION="eu-north-1"
ONEDRIVE_PATH="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-ValsoftCorporation/Sadie Shared - Sadie Lead Gen/USA/FL"

echo "=== Syncing from S3 to OneDrive ==="
mkdir -p "$ONEDRIVE_PATH"

aws s3 sync "$S3_EXPORTS" "$ONEDRIVE_PATH" --region $REGION

echo ""
echo "Done! Files synced to: $ONEDRIVE_PATH"
