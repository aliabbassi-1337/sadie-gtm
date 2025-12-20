#!/bin/zsh
# =============================================================================
# Sydney Multi-Query Scraper
# =============================================================================
# Runs multiple search queries across multiple API keys IN PARALLEL
# 
# Usage:
#   1. Add your API keys below
#   2. chmod +x run_sydney_multi.sh
#   3. ./run_sydney_multi.sh
#
# Options:
#   PARALLEL=5 ./run_sydney_multi.sh   # Run 5 queries at once (default: 3)
# =============================================================================

# Don't exit on error (parallel jobs may fail independently)
set +e

# Number of parallel workers (default 3 to be safe with rate limits)
PARALLEL_JOBS=${PARALLEL:-3}

# -----------------------------------------------------------------------------
# API KEYS - Add your free tier keys here
# -----------------------------------------------------------------------------
# Serper.dev: 2,500 free queries per account - https://serper.dev
# SerpAPI: 100 free queries/month per account - https://serpapi.com

SERPER_KEYS=(
    "c2033c70599227fdfd4078555bf6b608c9e82fd0"
    "a4428a317fb3c9d6443a81683a9113808ea08aa3"
    "9e76b3ae45f15107b6ccf7419b07cbd01eb6e915"
    "a14adcdd03fa71ebeab0678d8f1b5bde1f93b100",
    "82a942cea732ef53bcd34fee1109fb27905a061c"
)

SERPAPI_KEYS=(
    "12d2d5211b3e2af0a5af6e548b9bde4a856e84e0931861186c0eea0b144620bc"
)

# -----------------------------------------------------------------------------
# SEARCH QUERIES - Load from file
# -----------------------------------------------------------------------------
# Options:
#   sydney_queries_heavy.txt     - 4,133 queries, MAX COVERAGE (RECOMMENDED)
#   sydney_queries_optimized.txt - 656 queries, balanced
#   sydney_queries_expanded.txt  - 1,222 queries, all suburbs
#   sydney_queries.txt           - 613 queries, original list
QUERY_FILE="sydney_queries_heavy.txt"

# -----------------------------------------------------------------------------
# OUTPUT DIRECTORY (timestamped to avoid overwriting)
# -----------------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="scraper_output/sydney_multi_${TIMESTAMP}"

# Create output dir and failed log
mkdir -p "$OUTPUT_DIR"
FAILED_LOG="$OUTPUT_DIR/failed_queries.log"
touch "$FAILED_LOG"

# Read queries from file (skip comments and empty lines)
# Works with zsh and bash 4+
QUERIES=()
while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" == \#* ]] && continue
    # Trim whitespace
    line="${line## }"
    line="${line%% }"
    [[ -n "$line" ]] && QUERIES+=("$line")
done < "$QUERY_FILE"

# -----------------------------------------------------------------------------
# BUILD COMBINED KEY LIST
# -----------------------------------------------------------------------------
# Format: "provider:key" so we know which scraper to use

ALL_KEYS=()

# Add Serper keys
for key in "${SERPER_KEYS[@]}"; do
    [[ -n "$key" && "$key" != YOUR_* ]] && ALL_KEYS+=("serper:$key")
done

# Add SerpAPI keys
for key in "${SERPAPI_KEYS[@]}"; do
    [[ -n "$key" && "$key" != YOUR_* ]] && ALL_KEYS+=("serpapi:$key")
done

# -----------------------------------------------------------------------------
# RUN SCRAPER
# -----------------------------------------------------------------------------
echo "=============================================="
echo "Sydney Multi-Query Scraper (PARALLEL)"
echo "=============================================="
echo "Total queries: ${#QUERIES[@]}"
echo "Serper keys: ${#SERPER_KEYS[@]}"
echo "SerpAPI keys: ${#SERPAPI_KEYS[@]}"
echo "Total keys: ${#ALL_KEYS[@]}"
echo "Parallel jobs: $PARALLEL_JOBS"
echo "Output directory: $OUTPUT_DIR"
echo "=============================================="
echo ""

# Check if any keys are set
if [[ ${#ALL_KEYS[@]} -eq 0 ]]; then
    echo "ERROR: No API keys configured!"
    echo ""
    echo "Add keys to this script:"
    echo "  - Serper.dev: 2,500 free queries - https://serper.dev"
    echo "  - SerpAPI: 100 free queries/month - https://serpapi.com"
    echo ""
    exit 1
fi

echo "Using keys:"
for key_info in "${ALL_KEYS[@]}"; do
    provider="${key_info%%:*}"
    key="${key_info#*:}"
    echo "  - $provider: ${key:0:8}..."
done
echo ""

# Distribute queries across all keys
key_count=${#ALL_KEYS[@]}
query_count=${#QUERIES[@]}

# Track running jobs
running_jobs=0

# Use integer index for zsh compatibility
for (( i=1; i<=${#QUERIES[@]}; i++ )); do
    query="${QUERIES[$i]}"
    
    # Round-robin key selection
    key_index=$(( (i-1) % key_count + 1 ))
    key_info="${ALL_KEYS[$key_index]}"
    provider="${key_info%%:*}"
    api_key="${key_info#*:}"
    
    # Create safe filename from query
    safe_name=$(echo "$query" | tr ' ' '_' | tr -cd '[:alnum:]_')
    output_file="$OUTPUT_DIR/${safe_name}.csv"
    
    # Run scraper in background (inline, not function)
    (
        if [[ "$provider" == "serper" ]]; then
            python3 sadie_scraper_serper.py \
                --query "$query" \
                --api-key "$api_key" \
                --no-neighborhoods \
                --output "$output_file" >/dev/null 2>&1
        else
            python3 sadie_scraper_serpapi.py \
                --query "$query" \
                --api-key "$api_key" \
                --output "$output_file" >/dev/null 2>&1
        fi
        
        # Check results
        if [[ -f "$output_file" ]]; then
            lines=$(wc -l < "$output_file" | tr -d ' ')
            lines=$((lines - 1))
        else
            lines=0
        fi
        
        if [[ $lines -le 0 ]]; then
            echo "[$i/$query_count] ✗ $query (0 results)"
            echo "$query" >> "$FAILED_LOG"
        else
            echo "[$i/$query_count] ✓ $query ($lines hotels)"
        fi
    ) &
    
    ((running_jobs++))
    
    # Wait if we've hit max parallel jobs
    if [[ $running_jobs -ge $PARALLEL_JOBS ]]; then
        wait -n 2>/dev/null || wait
        ((running_jobs--))
    fi
    
    # Small stagger
    sleep 0.2
done

# Wait for all remaining jobs
echo ""
echo "Waiting for remaining jobs to complete..."
wait

echo ""
echo "All queries complete!"

echo ""
echo "=============================================="
echo "DONE! Merging results..."
echo "=============================================="

# Merge all CSVs into one
MERGED_FILE="scraper_output/sydney_multi_merged_${TIMESTAMP}.csv"

# Get header from first file
first_file=$(ls "$OUTPUT_DIR"/*.csv 2>/dev/null | head -1)
if [[ -n "$first_file" ]]; then
    head -1 "$first_file" > "$MERGED_FILE"
    
    # Append data from all files (skip headers)
    for f in "$OUTPUT_DIR"/*.csv; do
        tail -n +2 "$f" >> "$MERGED_FILE"
    done
    
    # Count results
    total_lines=$(($(wc -l < "$MERGED_FILE") - 1))
    echo "Merged $total_lines hotels into: $MERGED_FILE"
else
    echo "No output files found!"
fi

# Show stats
echo ""
echo "=============================================="
echo "SUMMARY"
echo "=============================================="
successful_files=$(find "$OUTPUT_DIR" -name "*.csv" -exec sh -c 'test $(wc -l < "$1") -gt 1' _ {} \; -print 2>/dev/null | wc -l)
failed_count=$(wc -l < "$FAILED_LOG" 2>/dev/null || echo "0")
echo "Successful queries: $successful_files"
echo "Failed queries (0 results): $failed_count"
if [[ -f "$MERGED_FILE" ]]; then
    total_hotels=$(($(wc -l < "$MERGED_FILE") - 1))
    echo "Total hotels (before dedup): $total_hotels"
fi
echo ""
echo "Failed queries logged to: $FAILED_LOG"
echo ""
echo "Next steps:"
echo "  1. Deduplicate: python3 sadie_postprocess.py $MERGED_FILE"
echo "  2. Enrich: python3 sadie_enricher.py ..."
echo "  3. Detect: python3 sadie_detector.py ..."
echo ""

