#!/bin/bash
# Run detection locally
set -e

CONCURRENCY=10

CITIES=(
    miami_beach kissimmee miami pensacola fort_lauderdale
    tampa saint_augustine key_west windermere panama_city_beach
    bay_pines orlando daytona_beach north_miami_beach pompano_beach
    homestead fort_myers_beach hialeah saint_petersburg clearwater_beach
    jacksonville sarasota pembroke_pines fort_myers high_springs
)

mkdir -p detector_output/florida_local logs/florida_local

echo "[$(date +%H:%M:%S)] Starting detection (25 cities parallel)..."
for city in "${CITIES[@]}"; do
    [ -f "scraper_output/florida/${city}.csv" ] || { echo "  Missing: ${city}"; continue; }
    echo "  Starting: ${city}"
    uv run python scripts/pipeline/detect.py \
        --input "scraper_output/florida/${city}.csv" \
        --output "detector_output/florida_local/${city}_leads.csv" \
        --concurrency $CONCURRENCY \
        > "logs/florida_local/${city}.log" 2>&1 &
done

echo "[$(date +%H:%M:%S)] All jobs launched. Logs in logs/florida_local/"
echo "  Monitor with: tail -f logs/florida_local/*.log"
wait

echo ""
echo "[$(date +%H:%M:%S)] Detection complete. Results:"
for city in "${CITIES[@]}"; do
    if [ -f "detector_output/florida_local/${city}_leads.csv" ]; then
        count=$(wc -l < "detector_output/florida_local/${city}_leads.csv")
        echo "  ✓ ${city}: $((count - 1)) leads"
    else
        echo "  ✗ ${city}: FAILED (check logs/florida_local/${city}.log)"
    fi
done

echo "[$(date +%H:%M:%S)] Done!"
