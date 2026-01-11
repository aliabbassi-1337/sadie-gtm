#!/bin/bash
#
# Sadie Lead Generation Pipeline
# ===============================
# Runs: Detect → Post-process → Enrich → Export → Funnel Stats
# 
# Usage:
#   ./run_pipeline.sh scraper_output/florida
#   ./run_pipeline.sh scraper_output/florida/* --country USA
#   ./run_pipeline.sh scraper_output/florida --skip-room-count
#   ./run_pipeline.sh scraper_output/florida/marco_island_hotels.csv
#

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "[$(date +%H:%M:%S)] ${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "[$(date +%H:%M:%S)] ${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "[$(date +%H:%M:%S)] ${RED}[ERROR]${NC} $1"; }
header() { echo -e "\n${BLUE}========== $1 ==========${NC}"; }

# OneDrive base path
ONEDRIVE_BASE="$HOME/Library/CloudStorage/OneDrive-ValsoftCorporation/Sadie Lead Gen"

# Parse flags and collect input paths
SKIP_DETECT=false
SKIP_ROOM_COUNT=false
SKIP_CUSTOMER=false
SKIP_EXPORT=false
CONCURRENCY=10
COUNTRY="USA"
OUTPUT_STATE=""
INPUT_PATHS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-detect) SKIP_DETECT=true; shift ;;
        --skip-room-count) SKIP_ROOM_COUNT=true; shift ;;
        --skip-customer) SKIP_CUSTOMER=true; shift ;;
        --skip-export) SKIP_EXPORT=true; shift ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --country) COUNTRY="$2"; shift 2 ;;
        --state) OUTPUT_STATE="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: ./run_pipeline.sh <files_or_folders...> [options]"
            echo ""
            echo "Options:"
            echo "  --state <name>       Output state folder name (e.g. 'florida')"
            echo "  --skip-detect        Skip detection (use existing detector output)"
            echo "  --skip-room-count    Skip room count enrichment"
            echo "  --skip-customer      Skip customer enrichment"
            echo "  --skip-export        Skip Excel export"
            echo "  --concurrency <n>    Detector concurrency (default: 10)"
            echo "  --country <name>     Country for OneDrive folder (default: USA)"
            echo ""
            echo "Examples:"
            echo "  ./run_pipeline.sh scraper_output/florida"
            echo "  ./run_pipeline.sh scraper_output/florida/* --country USA"
            echo "  ./run_pipeline.sh scraper_output/florida/miami.csv scraper_output/florida/tampa.csv"
            echo "  ./run_pipeline.sh scraper_output/florida --skip-room-count"
            echo "  ./run_pipeline.sh scraper_output/australia --country Australia"
            exit 0
            ;;
        -*) warn "Unknown option: $1"; shift ;;
        *) INPUT_PATHS+=("$1"); shift ;;
    esac
done

if [ ${#INPUT_PATHS[@]} -eq 0 ]; then
    echo "Usage: ./run_pipeline.sh <files_or_folders...> [options]"
    echo "Run './run_pipeline.sh --help' for more info"
    exit 1
fi

# Collect all CSV files from input paths
FILES=()
for INPUT_PATH in "${INPUT_PATHS[@]}"; do
    if [ -d "$INPUT_PATH" ]; then
        while IFS= read -r f; do
            FILES+=("$f")
        done < <(find "$INPUT_PATH" -maxdepth 1 -name "*.csv" -type f | sort)
    elif [ -f "$INPUT_PATH" ] && [[ "$INPUT_PATH" == *.csv ]]; then
        FILES+=("$INPUT_PATH")
    elif [ ! -e "$INPUT_PATH" ]; then
        warn "Not found: $INPUT_PATH"
    fi
done

if [ ${#FILES[@]} -eq 0 ]; then
    error "No CSV files found"
    exit 1
fi

log "Found ${#FILES[@]} CSV file(s) to process"

# Process each file
for SCRAPER_FILE in "${FILES[@]}"; do
    FILENAME=$(basename "$SCRAPER_FILE" .csv)
    CITY_SLUG="${FILENAME%_hotels}"  # Remove _hotels suffix if present
    CITY_SLUG="${CITY_SLUG%_leads}"  # Remove _leads suffix if present
    # Remove technical suffixes (grid, osm, serper, zipcode, etc.)
    CITY_SLUG="${CITY_SLUG%_grid}"
    CITY_SLUG="${CITY_SLUG%_osm}"
    CITY_SLUG="${CITY_SLUG%_serper}"
    CITY_SLUG="${CITY_SLUG%_zipcode}"
    CITY_SLUG="${CITY_SLUG%_scraped}"
    
    header "Processing: $FILENAME"

    # Get folder name from file's parent directory
    FOLDER_NAME=$(basename "$(dirname "$SCRAPER_FILE")")

    # Output paths - use --state flag or extract from folder/filename
    if [ -n "$OUTPUT_STATE" ]; then
        STATE_SLUG="$OUTPUT_STATE"
    elif [ "$FOLDER_NAME" = "scraper_output" ]; then
        # File is directly in scraper_output/, try to extract state from filename
        STATE_SLUG=$(echo "$CITY_SLUG" | cut -d'_' -f1)
    else
        STATE_SLUG="$FOLDER_NAME"
    fi
    DETECTOR_DIR="detector_output/${STATE_SLUG}"
    
    # State name from slug (florida -> Florida) - macOS compatible
    STATE_NAME=$(echo "$STATE_SLUG" | sed 's/_/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2))}1')
    ONEDRIVE_DIR="${ONEDRIVE_BASE}/${COUNTRY}/${STATE_NAME}"
    mkdir -p "$DETECTOR_DIR" "$ONEDRIVE_DIR"
    
    # Single output file - all steps update in-place
    LEADS_FILE="${DETECTOR_DIR}/${CITY_SLUG}_leads.csv"
    # Excel filename: Clean city name (Marco Island.xlsx, not marco_island_leads.xlsx)
    CITY_NAME=$(echo "$CITY_SLUG" | sed 's/_/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2))}1')
    EXCEL_FILE="${ONEDRIVE_DIR}/${CITY_NAME}.xlsx"

    log "Scraper:  $SCRAPER_FILE"
    log "Leads:    $LEADS_FILE"
    log "OneDrive: $EXCEL_FILE"

    # Skip if output already exists
    if [ -f "$LEADS_FILE" ] && [ "$SKIP_DETECT" = false ]; then
        EXISTING_LINES=$(($(wc -l < "$LEADS_FILE") - 1))
        if [ "$EXISTING_LINES" -gt 0 ]; then
            warn "Skipping $CITY_SLUG - already has $EXISTING_LINES leads. Use --skip-detect to re-process."
            continue
        fi
    fi

    # Step 1: Detect
    if [ "$SKIP_DETECT" = false ]; then
        header "DETECTION"
        python3 scripts/pipeline/detect.py \
            --input "$SCRAPER_FILE" \
            --output "$LEADS_FILE" \
            --concurrency "$CONCURRENCY"
    else
        log "Skipping detection (using existing leads file)"
        if [ ! -f "$LEADS_FILE" ]; then
            error "Leads file not found: $LEADS_FILE"
            continue
        fi
    fi

    # Step 2: Post-process (updates in-place)
    header "POST-PROCESSING"
    python3 scripts/pipeline/postprocess.py "$LEADS_FILE"

    # Step 3: Room count enrichment (updates in-place)
    if [ "$SKIP_ROOM_COUNT" = false ]; then
        header "ROOM COUNT ENRICHMENT (Groq)"
        python3 scripts/enrichers/room_count_groq.py --input "$LEADS_FILE" --concurrency 25 || warn "Groq room count failed"

        header "ROOM COUNT ENRICHMENT (Google AI fallback)"
        python3 scripts/enrichers/room_count_google.py --input "$LEADS_FILE" --concurrency 15 || warn "Google AI room count failed"
    fi

    # Step 4: Customer enrichment (updates in-place)
    if [ "$SKIP_CUSTOMER" = false ]; then
        header "CUSTOMER ENRICHMENT"
        python3 scripts/enrichers/customer_match.py --input "$LEADS_FILE" || warn "Customer enrichment failed"
    fi

    # Step 5: Excel export
    if [ "$SKIP_EXPORT" = false ]; then
        header "EXCEL EXPORT"
        python3 scripts/pipeline/export_excel.py \
            --input "$LEADS_FILE" \
            --city "$CITY_NAME" \
            --scraper "$SCRAPER_FILE" \
            --output "$EXCEL_FILE" || warn "Excel export failed"
    fi

    # Step 6: Funnel stats
    header "FUNNEL STATS"
    python3 scripts/pipeline/funnel_stats.py \
        --city "$CITY_SLUG" \
        --scraper "$SCRAPER_FILE" \
        --detector "$LEADS_FILE" \
        --output "${DETECTOR_DIR}/${CITY_SLUG}_funnel_stats.csv" || warn "Funnel stats failed"

    # Summary
    if [ -f "$LEADS_FILE" ]; then
        LEADS=$(($(wc -l < "$LEADS_FILE") - 1))
        log "✓ Complete: $LEADS leads → $EXCEL_FILE"
    fi
    echo ""
done

# Generate country stats
header "UPDATING COUNTRY STATS"
COUNTRY_DIR="${ONEDRIVE_BASE}/${COUNTRY}"
if [ -d "$COUNTRY_DIR" ]; then
    # Convert COUNTRY to lowercase for script (USA -> usa)
    COUNTRY_LOWER=$(echo "$COUNTRY" | tr '[:upper:]' '[:lower:]')
    python3 scripts/pipeline/country_stats.py --country "$COUNTRY_LOWER" || warn "Country stats failed"
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}PIPELINE COMPLETE!${NC}"
echo -e "${GREEN}========================================${NC}"
