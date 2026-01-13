#!/bin/bash
# Run pipeline on top 25 CEO cities only

CITIES=(
    miami_beach kissimmee miami pensacola fort_lauderdale
    tampa saint_augustine key_west windermere panama_city_beach
    bay_pines orlando daytona_beach north_miami_beach pompano_beach
    homestead fort_myers_beach hialeah saint_petersburg clearwater_beach
    jacksonville sarasota pembroke_pines fort_myers high_springs
)

# Build list of scraper files
FILES=()
for city in "${CITIES[@]}"; do
    f="scraper_output/florida/${city}.csv"
    if [ -f "$f" ]; then
        FILES+=("$f")
    else
        echo "Warning: $f not found"
    fi
done

echo "Processing ${#FILES[@]} CEO cities..."

# Pass all files and any extra arguments to run_pipeline.sh
./run_pipeline.sh "${FILES[@]}" "$@"
