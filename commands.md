# Sadie Lead Gen - Commands

## Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browser
python3 -m playwright install chromium
```

## Environment

Create a `.env` file with your Google Places API key (only needed for scraper):

```
GOOGLE_PLACES_API_KEY=your_api_key_here
```

---

## Option 1: Separate Scripts (Recommended)

### Scraper (Google Places API)

```bash
# Scrape hotels in Miami
python3 sadie_scraper.py

# Custom location
python3 sadie_scraper.py --center-lat 34.0522 --center-lng -118.2437 --overall-radius-km 40

# Limit results
python3 sadie_scraper.py --max-results 50
```

Output: `hotels_scraped.csv`

### Detector (Booking Engine Detection)

```bash
# Detect booking engines from scraped hotels
python3 sadie_detector.py --input hotels_scraped.csv

# Use manual hotel list
python3 sadie_detector.py --input hotels_manual.csv

# Show browser for debugging
python3 sadie_detector.py --input hotels_manual.csv --headed

# Custom output
python3 sadie_detector.py --input hotels_manual.csv --output my_leads.csv
```

Output: `sadie_leads.csv` + `screenshots/`

---

## Option 2: Unified Script

### Full Pipeline (Scrape + Detect)

```bash
# Miami (default)
python3 sadie_lead_gen.py

# Custom location
python3 sadie_lead_gen.py --center-lat 34.0522 --center-lng -118.2437 --overall-radius-km 40
```

### Detection Only (Skip Scraping)

```bash
# Use existing CSV
python3 sadie_lead_gen.py --skip-scrape --input hotels_manual.csv

# With browser visible
python3 sadie_lead_gen.py --skip-scrape --input hotels_manual.csv --headed
```

---

## CSV Format (minimum required)

```csv
name,website
The Setai Miami Beach,https://www.thesetaihotel.com
Fontainebleau Miami Beach,https://www.fontainebleau.com
```

Optional columns: `latitude`, `longitude`, `phone`, `address`, `rating`, `review_count`, `place_id`

---

## Performance Tuning

```bash
# More parallel browsers (faster)
python3 sadie_detector.py --input hotels.csv --concurrency 10

# Slower, more polite
python3 sadie_detector.py --input hotels.csv --concurrency 3 --pause 1.5

# Debug mode
python3 sadie_detector.py --input hotels.csv --headed --concurrency 1
```

---

## Output Files

| File | Description |
|------|-------------|
| `hotels_scraped.csv` | Hotels from Google Places scraper |
| `sadie_leads.csv` | Final output with booking engine data |
| `screenshots/` | Booking page screenshots |
| `sadie_scraper.log` | Scraper log |
| `sadie_detector.log` | Detector log |

---

## Common Locations

| City | Lat | Lng |
|------|-----|-----|
| Miami | 25.7617 | -80.1918 |
| Los Angeles | 34.0522 | -118.2437 |
| New York | 40.7128 | -74.0060 |
| Las Vegas | 36.1699 | -115.1398 |
| Orlando | 28.5383 | -81.3792 |
| San Francisco | 37.7749 | -122.4194 |
| Chicago | 41.8781 | -87.6298 |
| Austin | 30.2672 | -97.7431 |
| Denver | 39.7392 | -104.9903 |
| Seattle | 47.6062 | -122.3321 |
