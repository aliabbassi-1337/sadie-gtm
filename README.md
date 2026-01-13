<p align="center">
  <img src="graphics/sadie-gtm-logo.svg" width="300" alt="Sadie GTM Logo">
</p>

# Sadie GTM Pipeline

Automated lead generation pipeline for hotel booking engine detection at scale. Refactored to process nationwide with parallel processing and smart filtering.

## Architecture

```mermaid
flowchart TB
    subgraph Stage1[Stage 1: Scrape]
        CLI[CLI Input<br/>Cities/States]
        SCRAPE_Q[scrape-queue<br/>AWS SQS]
        SCRAPE_W[Scrape Workers<br/>EC2 x3]
    end

    subgraph Stage2[Stage 2: Detect]
        DETECT_D[Detect Dispatcher<br/>Query: status=0]
        DETECT_Q[detect-queue<br/>AWS SQS]
        DETECT_W[Detect Workers<br/>EC2 x3]
    end

    subgraph Stage3[Stage 3: Enrich - Tier 1 Only]
        ENRICH_D[Enrich Dispatcher<br/>Query: status=1 AND tier=1]
        ENRICH_Q1[enrich-room-queue<br/>AWS SQS]
        ENRICH_Q2[enrich-proximity-queue<br/>AWS SQS]
        ENRICH_Q3[enrich-research-queue<br/>AWS SQS]
        ENRICH_W1[Room Workers<br/>EC2 x2]
        ENRICH_W2[Proximity Workers<br/>EC2 x2]
        ENRICH_W3[Research Workers<br/>EC2 x2]
    end

    subgraph Stage4[Stage 4: Score]
        SCORE_D[Score Dispatcher<br/>Query: status=3]
        SCORE_Q[score-queue<br/>AWS SQS]
        SCORE_W[Score Workers<br/>EC2 x1]
    end

    subgraph Stage5[Stage 5: Launch]
        LAUNCH[Launcher Job<br/>Daily Cron<br/>Query: status=4]
    end

    subgraph Stage6[Stage 6: Export]
        EXPORT_D[Export Dispatcher<br/>Query: status=5]
        EXPORT_Q[export-queue<br/>AWS SQS]
        EXPORT_W[Export Workers<br/>EC2 x3]
    end

    subgraph Storage
        DB[(Supabase<br/>PostgreSQL)]
        S3[S3 Storage<br/>Website Content<br/>Logs]
        ONEDRIVE[OneDrive<br/>Excel Files]
    end

    CLI --> SCRAPE_Q
    SCRAPE_Q --> SCRAPE_W
    SCRAPE_W --> DB

    DB --> DETECT_D
    DETECT_D --> DETECT_Q
    DETECT_Q --> DETECT_W
    DETECT_W --> DB
    DETECT_W --> S3

    DB --> ENRICH_D
    ENRICH_D --> ENRICH_Q1
    ENRICH_D --> ENRICH_Q2
    ENRICH_D --> ENRICH_Q3
    ENRICH_Q1 --> ENRICH_W1
    ENRICH_Q2 --> ENRICH_W2
    ENRICH_Q3 --> ENRICH_W3
    ENRICH_W1 --> DB
    ENRICH_W2 --> DB
    ENRICH_W3 --> DB

    DB --> SCORE_D
    SCORE_D --> SCORE_Q
    SCORE_Q --> SCORE_W
    SCORE_W --> DB

    DB --> LAUNCH
    LAUNCH --> DB

    DB --> EXPORT_D
    EXPORT_D --> EXPORT_Q
    EXPORT_Q --> EXPORT_W
    EXPORT_W --> ONEDRIVE
    EXPORT_W --> DB

    style ENRICH_D fill:#ff9999
    style Stage3 fill:#ffe6e6
```

### Pipeline Overview

This is a 6-stage pipeline using AWS SQS and EC2 consumers that will help us scrape, detect, and enrich the state of Florida, the entire United States, and other countries. The dispatchers only dispatch what we're interested in—the enrichment dispatcher, for example, will only dispatch messages for tier 1 booking engines, reducing the number of hotels for enrichment and hence saving on AI agent costs.

---

## Legacy Single-Script Usage

Lead generation tool for hotel booking engine detection. Scrapes hotels from Google Places API and detects which booking engine they use.

## Setup

```bash
pip install -r requirements.txt
python3 -m playwright install chromium
```

Create `.env` file (for scraper only):
```
GOOGLE_PLACES_API_KEY=your_key_here
```

## Usage

See [commands.md](commands.md) for all commands.

**Quick start:**
```bash
# Scrape hotels in Miami
python3 sadie_scraper.py

# Detect booking engines
python3 sadie_detector.py --input hotels_scraped.csv
```

## Input CSV Format

Minimum required:
```csv
name,website
The Setai Miami Beach,https://www.thesetaihotel.com
Fontainebleau Miami Beach,https://www.fontainebleau.com
```

Optional columns: `phone`, `address`, `latitude`, `longitude`, `rating`, `review_count`, `place_id`

## Output

| File | Description |
|------|-------------|
| `hotels_scraped.csv` | Hotels from Google Places |
| `sadie_leads.csv` | Final output with booking engine data |
| `screenshots/` | Booking page screenshots |

### Output Columns

| Column | Description |
|--------|-------------|
| `name` | Hotel name |
| `website` | Hotel website URL |
| `booking_url` | URL of booking engine page |
| `booking_engine` | Detected engine (SynXis, Cloudbeds, etc.) |
| `booking_engine_domain` | Domain of booking engine |
| `detection_method` | How engine was detected |
| `error` | Error message if any |
| `phone_google` | Phone from Google Places |
| `phone_website` | Phone scraped from website |
| `email` | Email scraped from website |
| `screenshot_path` | Screenshot filename |

## Supported Booking Engines

- Cloudbeds
- Mews
- SynXis / TravelClick
- Little Hotelier
- WebRezPro
- InnRoad
- ResNexus
- Newbook
- RMS Cloud
- RoomRaccoon
- SiteMinder / TheBookingButton
- Sabre / CRS
- eZee

## How Detection Works

1. Load hotel homepage
2. Find and click "Book Now" button
3. Navigate to booking page
4. Sniff network requests for booking engine domains
5. Take screenshot as proof

## Scripts

| Script | Purpose |
|--------|---------|
| `sadie_scraper.py` | Scrape hotels from Google Places API |
| `sadie_detector.py` | Detect booking engines from hotel websites |



## Common Locations

| City | Lat | Lng |
|------|-----|-----|
| Miami | 25.7617 | -80.1918 |
| Los Angeles | 34.0522 | -118.2437 |
| New York | 40.7128 | -74.0060 |
| Las Vegas | 36.1699 | -115.1398 |
| Orlando | 28.5383 | -81.3792 |