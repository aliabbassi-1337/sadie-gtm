#!/bin/bash
# Run detection on EC2 - pulls from S3, detects, pushes back
set -e

S3_BUCKET="sadie-gtm"
CONCURRENCY=10

# Top 25 CEO cities
CITIES=(
    miami_beach kissimmee miami pensacola fort_lauderdale
    tampa saint_augustine key_west windermere panama_city_beach
    bay_pines orlando daytona_beach north_miami_beach pompano_beach
    homestead fort_myers_beach hialeah saint_petersburg clearwater_beach
    jacksonville sarasota pembroke_pines fort_myers high_springs
)

echo "[$(date +%H:%M:%S)] Pulling from S3..."
for city in "${CITIES[@]}"; do
    aws s3 cp "s3://${S3_BUCKET}/scraper_output/florida/${city}.csv" "scraper_output/florida/" 2>/dev/null || echo "  Missing: ${city}"
done

echo "[$(date +%H:%M:%S)] Starting detection (25 cities parallel)..."
for city in "${CITIES[@]}"; do
    [ -f "scraper_output/florida/${city}.csv" ] || continue
    uv run python scripts/pipeline/detect.py \
        --input "scraper_output/florida/${city}.csv" \
        --output "detector_output/florida/${city}_leads.csv" \
        --concurrency $CONCURRENCY &
done
wait

echo "[$(date +%H:%M:%S)] Pushing results to S3..."
aws s3 sync detector_output/florida/ "s3://${S3_BUCKET}/detector_output/florida/"

echo "[$(date +%H:%M:%S)] Done!"
