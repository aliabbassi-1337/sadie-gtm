#!/bin/bash
# Sync reports from S3 to OneDrive/SharePoint
#
# Usage:
#   ./scripts/sync_reports.sh         # Sync all states + reports from S3 to OneDrive
#   ./scripts/sync_reports.sh TX      # Sync only Texas
#   ./scripts/sync_reports.sh FL TX   # Sync Florida and Texas

set -e

S3_BUCKET="sadie-gtm"
S3_BASE="s3://$S3_BUCKET/HotelLeadGen"
ONEDRIVE_BASE="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-ValsoftCorporation/Sadie Shared - Sadie Lead Gen"

# If specific states provided, use those; otherwise sync all
if [ $# -gt 0 ]; then
    STATES="$@"
else
    # Get all state directories from S3
    STATES=$(s5cmd ls "$S3_BASE/USA/*" | grep -E '/$' | sed 's|.*/USA/||' | tr -d '/')
fi

echo "=== Syncing from S3 to OneDrive (using s5cmd) ==="
echo "States: $STATES"
echo ""

# Sync all states concurrently
pids=()
for STATE in $STATES; do
    mkdir -p "$ONEDRIVE_BASE/USA/$STATE"
    echo "Starting sync: $STATE"
    s5cmd sync "$S3_BASE/USA/$STATE/*" "$ONEDRIVE_BASE/USA/$STATE/" &
    pids+=($!)
done

# Sync reports directory concurrently
mkdir -p "$ONEDRIVE_BASE/reports"
echo "Starting sync: reports"
s5cmd sync "$S3_BASE/reports/*" "$ONEDRIVE_BASE/reports/" &
pids+=($!)

# Wait for all syncs to complete
echo ""
echo "Waiting for all syncs to complete..."
for pid in "${pids[@]}"; do
    wait "$pid"
done

echo ""
echo "Done! Files synced to: $ONEDRIVE_BASE"
