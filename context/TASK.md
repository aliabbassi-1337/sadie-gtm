# Task: Hotel Lead Generation Scraper

## Project Purpose
Build a lead generation pipeline to find independent hotels (non-chain) for sales outreach. The pipeline scrapes Google Maps for hotels, detects their booking engine, and enriches with room counts and competitor proximity.

## Current Focus: Grid Scraper Service

### What We Built
A credit-efficient Google Maps scraper using Serper.dev API with adaptive grid-based searching.

**Location:** `services/leadgen/`
- `grid_scraper.py` - Core scraper with adaptive grid logic
- `service.py` - Service layer (scrape_region, scrape_state, estimate)
- `repo.py` - Database operations

**Workflow:** `workflows/scrape_region.py`

### Key Features
- **Adaptive grid subdivision** - Starts with 2km cells, subdivides dense areas
- **Location-based cell skipping** - Skips cells already covered by adjacent searches
- **Chain filtering** - Filters out Marriott, Hilton, booking.com, etc.
- **Incremental saving** - Saves after each batch (Ctrl+C safe)
- **Cost estimation** - `--estimate` flag shows API calls and cost before running

### Usage
```bash
# Estimate cost first
uv run python workflows/scrape_region.py --city miami_beach --radius-km 10 --estimate

# Run scrape
uv run python workflows/scrape_region.py --city miami_beach --radius-km 10

# Scrape entire state
uv run python workflows/scrape_region.py --state florida --estimate
```

### Serper API
- **Pricing:** $1 per 1,000 credits ($50 plan = 50k credits)
- **Rate limit:** 5 qps (free/basic), 50 qps ($50+ plans)
- **Endpoint:** `https://google.serper.dev/maps`
- **Key param:** `ll` for location-based search (`@lat,lng,zoom`)

## Alternative: Browser-Based Scraping

Reference implementation at `/Users/administrator/projects/google-maps-scraper` (Go project).

**How it works:**
1. Headless browser (Playwright) navigates Google Maps
2. Extracts `APP_INITIALIZATION_STATE` JavaScript object
3. Gets 33+ fields per business (more than Serper API)
4. Uses reverse-engineered RPC for reviews

**Tradeoffs:**
| Approach | Cost | Speed | Data | Reliability |
|----------|------|-------|------|-------------|
| Serper API | $1/1k queries | Fast (API) | 10-15 fields | High |
| Browser scraping | Free | Slow (browser) | 33+ fields | Medium (can break) |

## Pipeline Stages

```
1. SCRAPE     → Grid scraper finds hotels via Google Maps
2. DETECT     → Identify booking engine (Cloudbeds, Mews, etc.)
3. ENRICH     → Get room counts, competitor proximity
4. EXPORT     → Generate lead lists for sales
```

**Status field:**
- `0` = scraped (raw from Google Maps)
- `1` = detected (has booking engine info)
- `99` = no_booking_engine (filtered out)

## Next Steps
- [ ] Implement `detect_booking_engines` service function
- [ ] Add room count enrichment
- [ ] Add competitor proximity calculation
- [ ] Export workflow for sales team
