#!/bin/bash
# Scrape top 25 Florida cities from Serper (Google Maps)

CITIES=(
    "Miami, Florida"
    "Fort Lauderdale, Florida"
    "Miami Beach, Florida"
    "Pompano Beach, Florida"
    "Orlando, Florida"
    "Bradenton, Florida"
    "West Palm Beach, Florida"
    "Clearwater, Florida"
    "Marco Island, Florida"
    "Celebration, Florida"
    "Fort Myers, Florida"
    "Fort Myers Beach, Florida"
    "Clearwater Beach, Florida"
    "Naples, Florida"
    "St Augustine, Florida"
    "Destin, Florida"
    "Cape Coral, Florida"
    "Fort Walton Beach, Florida"
    "Panama City Beach, Florida"
    "Key West, Florida"
    "Amelia Island, Florida"
    "Pensacola, Florida"
    "Tampa, Florida"
    "Sarasota, Florida"
    "Jacksonville, Florida"
)

for city in "${CITIES[@]}"; do
    echo "========================================"
    echo "Scraping: $city"
    echo "========================================"
    python3 scripts/scrapers/serper.py --city "$city"
    sleep 2
done

echo "Done!"
