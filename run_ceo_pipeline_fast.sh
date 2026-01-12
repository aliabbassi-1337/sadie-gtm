#!/bin/bash
# Fast parallel pipeline for top 25 CEO cities
# Runs enrichment on all cities at once instead of one by one

set -e

CITIES=(
    miami_beach kissimmee miami pensacola fort_lauderdale
    tampa saint_augustine key_west windermere panama_city_beach
    bay_pines orlando daytona_beach north_miami_beach pompano_beach
    homestead fort_myers_beach hialeah saint_petersburg clearwater_beach
    jacksonville sarasota pembroke_pines fort_myers high_springs
)

SKIP_DETECT=false
SKIP_ROOM=false

# Parse args
for arg in "$@"; do
    case $arg in
        --skip-detect) SKIP_DETECT=true ;;
        --skip-room-count) SKIP_ROOM=true ;;
    esac
done

echo "[$(date +%H:%M:%S)] Starting fast CEO pipeline..."

# Step 1: Detection (parallel, 5 at a time)
if [ "$SKIP_DETECT" = false ]; then
    echo "[$(date +%H:%M:%S)] Running detection (5 parallel)..."
    count=0
    for city in "${CITIES[@]}"; do
        input="scraper_output/florida/${city}.csv"
        output="detector_output/florida/${city}_leads.csv"
        [ -f "$input" ] || continue

        uv run python scripts/pipeline/detect.py \
            --input "$input" \
            --output "$output" \
            --concurrency 10 &

        count=$((count + 1))
        if [ $((count % 5)) -eq 0 ]; then
            wait
        fi
    done
    wait
fi

# Step 2: Post-process all files
echo "[$(date +%H:%M:%S)] Post-processing..."
for city in "${CITIES[@]}"; do
    leads="detector_output/florida/${city}_leads.csv"
    [ -f "$leads" ] && python3 scripts/pipeline/postprocess.py "$leads" &
done
wait

# Step 3: Room count enrichment (all files at once with high concurrency)
if [ "$SKIP_ROOM" = false ]; then
    echo "[$(date +%H:%M:%S)] Room count enrichment (Groq)..."
    for city in "${CITIES[@]}"; do
        leads="detector_output/florida/${city}_leads.csv"
        [ -f "$leads" ] && python3 scripts/enrichers/room_count_groq.py --input "$leads" --concurrency 50 &
    done
    wait

    echo "[$(date +%H:%M:%S)] Room count enrichment (Google fallback)..."
    for city in "${CITIES[@]}"; do
        leads="detector_output/florida/${city}_leads.csv"
        [ -f "$leads" ] && python3 scripts/enrichers/room_count_google.py --input "$leads" --concurrency 15 &
    done
    wait
fi

# Step 4: Customer enrichment (parallel)
echo "[$(date +%H:%M:%S)] Customer enrichment..."
for city in "${CITIES[@]}"; do
    leads="detector_output/florida/${city}_leads.csv"
    [ -f "$leads" ] && python3 scripts/enrichers/customer_match.py --input "$leads" &
done
wait

# Step 5: Excel export (parallel)
echo "[$(date +%H:%M:%S)] Excel export..."
ONEDRIVE="$HOME/Library/CloudStorage/OneDrive-ValsoftCorporation/Sadie Lead Gen/USA/Florida"
for city in "${CITIES[@]}"; do
    leads="detector_output/florida/${city}_leads.csv"
    scraper="scraper_output/florida/${city}.csv"
    city_name=$(echo "$city" | sed 's/_/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2))}1')
    excel="$ONEDRIVE/${city_name}.xlsx"
    [ -f "$leads" ] && python3 scripts/pipeline/export_excel.py --input "$leads" --city "$city_name" --scraper "$scraper" --output "$excel" &
done
wait

echo "[$(date +%H:%M:%S)] Done!"
