#!/bin/bash
#
# AWS Detector Pipeline
# =====================
# Uploads scraper output to S3, runs detection on EC2, downloads results
#
# Prerequisites:
#   1. AWS CLI configured: aws configure
#   2. EC2 instance running with SSH access
#   3. S3 bucket: sadie-gtm
#
# Usage:
#   ./run_aws_detector.sh                    # Run on all Florida cities
#   ./run_aws_detector.sh --upload-only      # Just upload to S3
#   ./run_aws_detector.sh --detect-only      # Just run detection (data already on S3)
#   ./run_aws_detector.sh --download-only    # Just download results
#

set -e

# Configuration
S3_BUCKET="sadie-gtm"
EC2_HOST="${EC2_HOST:-sadie-ec2}"  # Set via env var or SSH config
EC2_USER="${EC2_USER:-ec2-user}"
STATE="florida"
CONCURRENCY=50

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }
header() { echo -e "\n${BLUE}=== $1 ===${NC}"; }

# Parse args
UPLOAD=true
DETECT=true
DOWNLOAD=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --upload-only) DETECT=false; DOWNLOAD=false; shift ;;
        --detect-only) UPLOAD=false; DOWNLOAD=false; shift ;;
        --download-only) UPLOAD=false; DETECT=false; shift ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --state) STATE="$2"; shift 2 ;;
        --ec2) EC2_HOST="$2"; shift 2 ;;
        *) shift ;;
    esac
done

# S3 paths
S3_SCRAPER="s3://${S3_BUCKET}/scraper_output/${STATE}"
S3_DETECTOR="s3://${S3_BUCKET}/detector_output/${STATE}"

# ============================================================================
# STEP 1: Upload scraper output to S3
# ============================================================================
if [ "$UPLOAD" = true ]; then
    header "UPLOADING TO S3"

    LOCAL_SCRAPER="scraper_output/${STATE}"
    if [ ! -d "$LOCAL_SCRAPER" ]; then
        echo "Error: $LOCAL_SCRAPER not found"
        exit 1
    fi

    # Count files
    FILE_COUNT=$(find "$LOCAL_SCRAPER" -maxdepth 1 -name "*.csv" ! -name "*_stats.csv" | wc -l | tr -d ' ')
    log "Uploading $FILE_COUNT CSV files to $S3_SCRAPER"

    aws s3 sync "$LOCAL_SCRAPER" "$S3_SCRAPER" \
        --exclude "*" \
        --include "*.csv" \
        --exclude "*_stats.csv" \
        --exclude "backup/*"

    log "Upload complete"
fi

# ============================================================================
# STEP 2: Run detection on EC2
# ============================================================================
if [ "$DETECT" = true ]; then
    header "RUNNING DETECTION ON EC2"

    log "Connecting to $EC2_HOST..."
    log "Concurrency: $CONCURRENCY"

    # Build the remote command
    REMOTE_SCRIPT=$(cat << 'REMOTE_EOF'
#!/bin/bash
set -e

STATE="__STATE__"
S3_BUCKET="__S3_BUCKET__"
CONCURRENCY="__CONCURRENCY__"

cd ~/sadie_gtm || cd ~/projects/sadie_gtm || { echo "Project dir not found"; exit 1; }

echo "[$(date +%H:%M:%S)] Syncing scraper data from S3..."
mkdir -p scraper_output/${STATE}
aws s3 sync s3://${S3_BUCKET}/scraper_output/${STATE} scraper_output/${STATE}

echo "[$(date +%H:%M:%S)] Starting detection..."
mkdir -p detector_output/${STATE}

# Process each city
for csv in scraper_output/${STATE}/*.csv; do
    [ -f "$csv" ] || continue
    [[ "$csv" == *_stats.csv ]] && continue

    filename=$(basename "$csv" .csv)
    output="detector_output/${STATE}/${filename}_leads.csv"

    if [ -f "$output" ]; then
        echo "  Skipping $filename (already exists)"
        continue
    fi

    echo "[$(date +%H:%M:%S)] Detecting: $filename"
    python3 scripts/pipeline/detect.py \
        --input "$csv" \
        --output "$output" \
        --concurrency $CONCURRENCY || echo "  Warning: $filename failed"
done

echo "[$(date +%H:%M:%S)] Uploading results to S3..."
aws s3 sync detector_output/${STATE} s3://${S3_BUCKET}/detector_output/${STATE}

echo "[$(date +%H:%M:%S)] Detection complete!"
REMOTE_EOF
)

    # Replace placeholders
    REMOTE_SCRIPT="${REMOTE_SCRIPT//__STATE__/$STATE}"
    REMOTE_SCRIPT="${REMOTE_SCRIPT//__S3_BUCKET__/$S3_BUCKET}"
    REMOTE_SCRIPT="${REMOTE_SCRIPT//__CONCURRENCY__/$CONCURRENCY}"

    # Execute on EC2
    ssh "${EC2_USER}@${EC2_HOST}" "$REMOTE_SCRIPT"

    log "Detection complete on EC2"
fi

# ============================================================================
# STEP 3: Download results from S3
# ============================================================================
if [ "$DOWNLOAD" = true ]; then
    header "DOWNLOADING RESULTS"

    LOCAL_DETECTOR="detector_output/${STATE}"
    mkdir -p "$LOCAL_DETECTOR"

    log "Downloading from $S3_DETECTOR"
    aws s3 sync "$S3_DETECTOR" "$LOCAL_DETECTOR"

    # Count results
    LEADS_COUNT=$(find "$LOCAL_DETECTOR" -name "*_leads.csv" | wc -l | tr -d ' ')
    log "Downloaded $LEADS_COUNT lead files"

    # Show summary
    header "SUMMARY"
    TOTAL_LEADS=0
    for f in "$LOCAL_DETECTOR"/*_leads.csv; do
        [ -f "$f" ] || continue
        count=$(($(wc -l < "$f") - 1))
        TOTAL_LEADS=$((TOTAL_LEADS + count))
    done
    log "Total leads detected: $TOTAL_LEADS"
fi

echo -e "\n${GREEN}Done!${NC}"
