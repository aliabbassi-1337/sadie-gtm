#!/bin/bash
# Export all Florida leads to Documents folder

mkdir -p ~/Documents/florida_export

for leads in detector_output/florida/*_leads.csv; do
    [ -f "$leads" ] || continue
    city_slug=$(basename "$leads" _leads.csv)
    city_name=$(echo "$city_slug" | sed 's/_/ /g' | awk '{for(i=1;i<=NF;i++) $i=toupper(substr($i,1,1)) tolower(substr($i,2))}1')
    scraper="scraper_output/florida/${city_slug}.csv"
    excel="$HOME/Documents/florida_export/${city_name}.xlsx"
    python3 scripts/pipeline/export_excel.py --input "$leads" --city "$city_name" --scraper "$scraper" --output "$excel" 2>/dev/null &
done

wait
echo "Done!"
ls ~/Documents/florida_export/*.xlsx | wc -l
