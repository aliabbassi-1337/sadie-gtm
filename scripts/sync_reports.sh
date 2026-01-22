#!/bin/bash
# Sync reports - upload to S3 and sync to OneDrive
#
# Usage:
#   ./scripts/sync.sh                    # Upload xlsx files and sync to OneDrive
#   ./scripts/sync.sh FL_dbpr.xlsx       # Upload specific file
#   ./scripts/sync.sh --download         # Just download from S3 to OneDrive

set -e

S3_BUCKET="sadie-gtm"
S3_EXPORTS="s3://$S3_BUCKET/exports"
S3_REPORTS="s3://$S3_BUCKET/HotelLeadGen"
REGION="eu-north-1"
ONEDRIVE_PATH="$HOME/Library/CloudStorage/OneDrive-SharedLibraries-ValsoftCorporation/Sadie Shared - Sadie Lead Gen"

# Check if just downloading
if [[ "$1" == "--download" ]]; then
    echo "Syncing from S3 to OneDrive..."
    mkdir -p "$ONEDRIVE_PATH"
    aws s3 sync "$S3_REPORTS" "$ONEDRIVE_PATH" --exclude "*.tmp"
    echo "Done! Files at: $ONEDRIVE_PATH"
    exit 0
fi

# Upload files
if [ -n "$1" ] && [ "$1" != "--download" ]; then
    FILES="$@"
else
    FILES=$(ls *.xlsx 2>/dev/null || true)
fi

if [ -z "$FILES" ]; then
    echo "No .xlsx files found to upload"
    exit 1
fi

echo "=== Uploading to S3 ==="
for file in $FILES; do
    if [ -f "$file" ]; then
        echo -n "  $file... "
        aws s3 cp "$file" "$S3_EXPORTS/$file" --region $REGION 2>/dev/null \
            && echo "OK" || echo "FAILED"
    fi
done

echo ""
echo "=== Download Links (valid 7 days) ==="
for file in $FILES; do
    if [ -f "$file" ]; then
        url=$(aws s3 presign "$S3_EXPORTS/$file" --region $REGION --expires-in 604800 2>/dev/null)
        echo "$file:"
        echo "  $url"
        echo ""
    fi
done

# Sync to OneDrive if it exists
if [ -d "$ONEDRIVE_PATH" ]; then
    echo "=== Syncing to OneDrive ==="
    cp $FILES "$ONEDRIVE_PATH/" 2>/dev/null && echo "Copied to OneDrive" || echo "OneDrive copy skipped"
fi
