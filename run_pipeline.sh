#!/bin/bash
# Sadie Lead Gen Pipeline
# Runs scraper -> enricher -> detector sequentially

set -e  # Exit on error

# ============================================================================
# CONFIGURATION - Edit these for your target location
# ============================================================================
LOCATION_NAME="gatlinburg"
LOCATION_LABEL="Gatlinburg TN"
CENTER_LAT="35.7125"
CENTER_LNG="-83.5373"
RADIUS_KM="100"
GRID_ROWS="10"
GRID_COLS="10"

# Concurrency settings
SCRAPER_CONCURRENCY="15"
ENRICHER_CONCURRENCY="5"
DETECTOR_CONCURRENCY="8"

# ============================================================================
# PATHS (auto-generated from location name)
# ============================================================================
SCRAPER_OUTPUT="scraper_output/${LOCATION_NAME}_hotels.csv"
ENRICHER_OUTPUT="enricher_output/${LOCATION_NAME}_hotels_enriched.csv"
DETECTOR_OUTPUT="detector_output/${LOCATION_NAME}_leads.csv"

# ============================================================================
# RUN PIPELINE
# ============================================================================
echo "============================================================"
echo "SADIE LEAD GEN PIPELINE"
echo "============================================================"
echo "Location:    $LOCATION_LABEL"
echo "Center:      $CENTER_LAT, $CENTER_LNG"
echo "Radius:      ${RADIUS_KM}km"
echo "Grid:        ${GRID_ROWS}x${GRID_COLS}"
echo "============================================================"
echo ""

# Create output directories
mkdir -p scraper_output enricher_output detector_output

# Step 1: Scraper
echo "============================================================"
echo "STEP 1/3: SCRAPING HOTELS"
echo "============================================================"
python3 sadie_scraper.py \
    --center-lat "$CENTER_LAT" \
    --center-lng "$CENTER_LNG" \
    --overall-radius-km "$RADIUS_KM" \
    --grid-rows "$GRID_ROWS" \
    --grid-cols "$GRID_COLS" \
    --concurrency "$SCRAPER_CONCURRENCY" \
    --output "$SCRAPER_OUTPUT"

echo ""
echo "✓ Scraper complete: $SCRAPER_OUTPUT"
echo ""

# Step 2: Enricher
echo "============================================================"
echo "STEP 2/3: ENRICHING MISSING WEBSITES"
echo "============================================================"
python3 sadie_enricher.py \
    --input "$SCRAPER_OUTPUT" \
    --output "$ENRICHER_OUTPUT" \
    --location "$LOCATION_LABEL" \
    --concurrency "$ENRICHER_CONCURRENCY"

echo ""
echo "✓ Enricher complete: $ENRICHER_OUTPUT"
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
echo "✓ Detector complete: $DETECTOR_OUTPUT"
echo ""

# Done
echo "============================================================"
echo "PIPELINE COMPLETE!"
echo "============================================================"
echo "Final leads: $DETECTOR_OUTPUT"
echo "Screenshots: screenshots/"
echo "============================================================"

