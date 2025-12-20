#!/bin/bash
# Sadie Lead Gen Pipeline - Sydney, Australia
# Uses FREE OSM scraper -> enricher -> detector

set -e

# ============================================================================
# CONFIGURATION
# ============================================================================
LOCATION_NAME="sydney"
LOCATION_LABEL="Sydney Australia"
CITY_QUERY="Sydney, Australia"
RADIUS_KM="50"

# Concurrency
ENRICHER_CONCURRENCY="10"
DETECTOR_CONCURRENCY="10"

# ============================================================================
# PATHS
# ============================================================================
SCRAPER_OUTPUT="scraper_output/${LOCATION_NAME}_hotels.csv"
ENRICHER_OUTPUT="enricher_output/${LOCATION_NAME}_hotels_enriched.csv"
DETECTOR_OUTPUT="detector_output/${LOCATION_NAME}_leads.csv"

# ============================================================================
# RUN PIPELINE
# ============================================================================
echo "============================================================"
echo "SADIE LEAD GEN PIPELINE (OSM)"
echo "============================================================"
echo "Location:    $LOCATION_LABEL"
echo "Radius:      ${RADIUS_KM}km"
echo "============================================================"
echo ""

mkdir -p scraper_output enricher_output detector_output

# Step 1: Scraper (OSM)
echo "============================================================"
echo "STEP 1/3: SCRAPING HOTELS (OSM)"
echo "============================================================"
python3 sadie_scraper_osm.py \
    --city "$CITY_QUERY" \
    --radius-km "$RADIUS_KM" \
    --output "$SCRAPER_OUTPUT"

echo ""

# Step 2: Enricher
echo "============================================================"
echo "STEP 2/3: ENRICHING WEBSITES"
echo "============================================================"
python3 sadie_enricher.py \
    --input "$SCRAPER_OUTPUT" \
    --output "$ENRICHER_OUTPUT" \
    --location "$LOCATION_LABEL" \
    --concurrency "$ENRICHER_CONCURRENCY"

echo ""

# Step 3: Detector
echo "============================================================"
echo "STEP 3/3: DETECTING BOOKING ENGINES"
echo "============================================================"
python3 sadie_detector.py \
    --input "$ENRICHER_OUTPUT" \
    --output "$DETECTOR_OUTPUT" \
    --concurrency "$DETECTOR_CONCURRENCY"

echo ""
echo "============================================================"
echo "PIPELINE COMPLETE!"
echo "============================================================"
echo "Final leads: $DETECTOR_OUTPUT"
echo "============================================================"
