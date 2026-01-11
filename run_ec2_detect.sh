#!/bin/bash
# Run detection on EC2 - pulls from S3, detects, pushes back
# TODO: Use s5cmd for faster parallel S3 transfers (https://github.com/peak/s5cmd)
#       Install: wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
#       Usage: s5cmd cp "s3://sadie-gtm/scraper_output/florida/*" scraper_output/florida/
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

mkdir -p logs/florida
BATCH_SIZE=5

echo "[$(date +%H:%M:%S)] Starting detection (${BATCH_SIZE} cities at a time)..."
count=0
for city in "${CITIES[@]}"; do
    [ -f "scraper_output/florida/${city}.csv" ] || continue
    echo "  Starting: ${city}"
    uv run python scripts/pipeline/detect.py \
        --input "scraper_output/florida/${city}.csv" \
        --output "detector_output/florida/${city}_leads.csv" \
        --concurrency $CONCURRENCY \
        > "logs/florida/${city}.log" 2>&1 &

    count=$((count + 1))
    if [ $((count % BATCH_SIZE)) -eq 0 ]; then
        echo "[$(date +%H:%M:%S)] Waiting for batch to complete..."
        wait
    fi
done
wait  # Wait for any remaining jobs

# Summary
echo ""
echo "[$(date +%H:%M:%S)] Detection complete. Results:"
for city in "${CITIES[@]}"; do
    if [ -f "detector_output/florida/${city}_leads.csv" ]; then
        count=$(wc -l < "detector_output/florida/${city}_leads.csv")
        echo "  ✓ ${city}: $((count - 1)) leads"
    else
        echo "  ✗ ${city}: FAILED (check logs/florida/${city}.log)"
    fi
done

echo "[$(date +%H:%M:%S)] Pushing results to S3..."
aws s3 sync detector_output/florida/ "s3://${S3_BUCKET}/detector_output/florida/"

echo "[$(date +%H:%M:%S)] Done!"
