# Sadie Lead Gen - Commands

## Setup

```bash
pip install -r requirements.txt
python3 -m playwright install chromium
```

## Scraper

```bash
# Miami (default)
python3 sadie_scraper.py

# Custom location
python3 sadie_scraper.py --center-lat 34.0522 --center-lng -118.2437 --overall-radius-km 40

# Limit results
python3 sadie_scraper.py --max-results 50
```

## Detector

```bash
# Basic
python3 sadie_detector.py --input hotels_manual.csv

# Debug mode (show browser)
python3 sadie_detector.py --input hotels_manual.csv --headed --concurrency 1

# Custom output
python3 sadie_detector.py --input hotels.csv --output my_leads.csv

# Faster
python3 sadie_detector.py --input hotels.csv --concurrency 10
```