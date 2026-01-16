#!/bin/bash
# Sync hotel lead reports from S3 to OneDrive
#
# Usage:
#   ./scripts/sync_reports.sh              # Sync all reports
#   ./scripts/sync_reports.sh --dry-run    # Preview what would be synced

set -e

S3_BUCKET="sadie-gtm"
S3_PREFIX="HotelLeadGen"
ONEDRIVE_PATH="$HOME/Library/CloudStorage/OneDrive-ValsoftCorporation/HotelLeadGen"

# Parse args
DRY_RUN=""
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN="--dryrun"
    echo "=== DRY RUN MODE ==="
fi

# Create OneDrive directory if it doesn't exist
mkdir -p "$ONEDRIVE_PATH"

echo "Syncing from s3://$S3_BUCKET/$S3_PREFIX to $ONEDRIVE_PATH"
echo ""

# Sync from S3 to OneDrive
aws s3 sync "s3://$S3_BUCKET/$S3_PREFIX" "$ONEDRIVE_PATH" \
    --exclude "*.tmp" \
    $DRY_RUN

echo ""
echo "Sync complete!"
echo "Reports available at: $ONEDRIVE_PATH"

# Restart OneDrive to force cloud sync
if [[ -z "$DRY_RUN" ]]; then
    echo ""
    echo "Restarting OneDrive..."
    killall OneDrive 2>/dev/null || true
    sleep 1
    open -a OneDrive
    echo "OneDrive restarted - files will sync to cloud shortly"
fi
