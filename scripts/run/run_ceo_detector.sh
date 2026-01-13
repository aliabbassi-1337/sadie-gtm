#!/bin/bash
#
# CEO Cities Detector - Top 5 Florida cities only
#
# Usage:
#   ./run_ceo_detector.sh
#

set -e

S3_BUCKET="sadie-gtm"
EC2_HOST="${EC2_HOST:-sadie-ec2}"
CONCURRENCY=50

# Top 25 CEO cities
CITIES=(
    miami_beach kissimmee miami pensacola fort_lauderdale
    tampa saint_augustine key_west windermere panama_city_beach
    bay_pines orlando daytona_beach north_miami_beach pompano_beach
    homestead fort_myers_beach hialeah saint_petersburg clearwater_beach
    jacksonville sarasota pembroke_pines fort_myers high_springs
)

GREEN='\033[0;32m'
NC='\033[0m'
log() { echo -e "${GREEN}[$(date +%H:%M:%S)]${NC} $1"; }

# 1. Upload only CEO cities to S3
log "Uploading 5 CEO cities to S3..."
for city in "${CITIES[@]}"; do
    aws s3 cp "scraper_output/florida/${city}.csv" "s3://${S3_BUCKET}/scraper_output/florida/${city}.csv"
    log "  Uploaded ${city}.csv"
done

# 2. Run detection on EC2
log "Running detection on EC2..."
CITIES_STR="${CITIES[*]}"
ssh "${EC2_HOST}" << REMOTE
set -e
cd ~/sadie_gtm

CITIES=(${CITIES_STR})

# Pull from S3
mkdir -p scraper_output/florida detector_output/florida
for city in "\${CITIES[@]}"; do
    aws s3 cp s3://${S3_BUCKET}/scraper_output/florida/\${city}.csv scraper_output/florida/
done

# Detect all cities in parallel
echo "[\$(date +%H:%M:%S)] Starting detection on all 25 cities in parallel..."
for city in "\${CITIES[@]}"; do
    python3 scripts/pipeline/detect.py \
        --input scraper_output/florida/\${city}.csv \
        --output detector_output/florida/\${city}_leads.csv \
        --concurrency 10 &
done
wait
echo "[\$(date +%H:%M:%S)] All cities complete!"

# Push results to S3
aws s3 sync detector_output/florida/ s3://${S3_BUCKET}/detector_output/florida/
echo "Done!"
REMOTE

# 3. Download results
log "Downloading results..."
mkdir -p detector_output/florida
for city in "${CITIES[@]}"; do
    aws s3 cp "s3://${S3_BUCKET}/detector_output/florida/${city}_leads.csv" "detector_output/florida/"
done

log "Complete! Results in detector_output/florida/"
