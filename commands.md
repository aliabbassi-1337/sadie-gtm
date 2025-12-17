# Sadie Lead Gen - Commands

## Full Pipeline (edit run_pipeline.sh for your location)

```bash
./run_pipeline.sh
```

## Scraper

```bash
python3 sadie_scraper.py --center-lat 38.3886 --center-lng -75.0735 --overall-radius-km 50 --output scraper_output/ocean_city_hotels.csv

python3 sadie_scraper.py --center-lat 38.3886 --center-lng -75.0735 --overall-radius-km 100 --grid-rows 10 --grid-cols 10 --output scraper_output/ocean_city_hotels.csv

python3 sadie_scraper.py --center-lat 35.7125 --center-lng -83.5373 --overall-radius-km 100 --grid-rows 10 --grid-cols 10 --output scraper_output/gatlinburg_hotels.csv

python3 sadie_scraper.py --center-lat 35.7125 --center-lng -83.5373 --overall-radius-km 150 --grid-rows 12 --grid-cols 12 --concurrency 25 --output scraper_output/gatlinburg_hotels.csv
```

## Enricher (uses DuckDuckGo - no CAPTCHAs!)

```bash
python3 sadie_enricher.py --input scraper_output/ocean_city_hotels.csv --output enricher_output/ocean_city_hotels_enriched.csv --location "Ocean City MD"

python3 sadie_enricher.py --input scraper_output/ocean_city_hotels.csv --output enricher_output/ocean_city_hotels_enriched.csv --location "Ocean City MD" --concurrency 8

python3 sadie_enricher.py --input scraper_output/gatlinburg_hotels.csv --output enricher_output/gatlinburg_hotels_enriched.csv --location "Gatlinburg TN" --concurrency 8


python3 sadie_enricher.py --input scraper_output/gatlinburg_hotels.csv --output enricher_output/gatlinburg_hotels_enriched.csv --location "Gatlinburg TN" --debug
```

## Detector

```bash
python3 sadie_detector.py --input enricher_output/ocean_city_hotels_enriched.csv --output detector_output/ocean_city_leads.csv

python3 sadie_detector.py --input enricher_output/ocean_city_hotels_enriched.csv --output detector_output/ocean_city_leads.csv --concurrency 10

python3 sadie_detector.py --input enricher_output/gatlinburg_hotels_enriched.csv --output detector_output/gatlinburg_leads.csv

python3 sadie_detector.py --input scraper_output/test_hotel.csv --output detector_output/test_leads.csv --headed --concurrency 1 --debug

python3 sadie_detector.py --input scraper_output/ocean_city_hotels.csv --output detector_output/ocean_city_leads.csv

python3 sadie_detector.py --input scraper_output/gatlinburg_hotels.csv --output detector_output/gatlinburg_leads.csv
```
