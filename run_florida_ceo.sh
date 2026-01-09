#!/bin/bash
# Scrape hotels for CEO's 27 Florida cities concurrently
# Usage: ./run_florida_ceo.sh

set -e

OUTPUT_DIR="scraper_output/florida_ceo"
mkdir -p "$OUTPUT_DIR"

echo "Scraping 27 CEO Florida cities..."
echo "Output: $OUTPUT_DIR"
echo ""

# CEO's 27 Florida cities
CITIES=(
    "Orlando"
    "Miami"
    "Miami Beach"
    "Fort Lauderdale"
    "Tampa"
    "West Palm Beach"
    "Key West"
    "St Petersburg"
    "Clearwater"
    "Naples"
    "Sarasota"
    "Jacksonville"
    "St Augustine"
    "Destin"
    "Panama City Beach"
    "Fort Myers"
    "Pensacola"
    "Kissimmee"
    "Cape Coral"
    "Marco Island"
    "Fort Walton Beach"
    "Bradenton"
    "Pompano Beach"
    "Fernandina Beach"
    "Clearwater Beach"
    "Palm Coast"
    "Flagler Beach"
)

# Launch all scrapers concurrently
for city in "${CITIES[@]}"; do
    echo "Starting: $city"
    python3 scripts/scrapers/city.py --city "$city" --state FL --output "$OUTPUT_DIR" &
done

echo ""
echo "All ${#CITIES[@]} scrapers launched. Waiting..."
wait

echo ""
echo "Done! Results in $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"
