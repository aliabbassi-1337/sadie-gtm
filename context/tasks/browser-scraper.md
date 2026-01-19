# Task: Browser-Based Google Maps Scraper

## Reference Implementation
`/Users/administrator/projects/google-maps-scraper` (Go project)

## Why Consider This?
- **Free** - No API costs (vs Serper at $1/1k queries)
- **More data** - 33+ fields per business (vs ~15 from Serper)
- **Reviews** - Can fetch up to ~300 reviews per place
- **No rate limits** - With proxies, unlimited scraping

## How It Works

### Architecture
```
SearchJob (HTTP) → GmapJob (Browser) → PlaceJob (JS extraction) → EmailExtractJob (optional)
```

### Key Technique: JavaScript State Extraction
Instead of parsing HTML, extracts Google's internal `APP_INITIALIZATION_STATE` object:
```javascript
(function() {
    if (!window.APP_INITIALIZATION_STATE) return null;
    return window.APP_INITIALIZATION_STATE[3];
})()
```

### Tech Stack
- **Language:** Go
- **Browser:** Playwright-Go (headless Chrome)
- **Framework:** scrapemate (job-based scraping)
- **Database:** PostgreSQL (distributed mode)

## Data Fields (33+)
- Basic: name, address, phone, website, category
- Location: lat/lng, Plus Code, timezone
- Ratings: rating, review_count, breakdown by stars
- Business: hours, status, price range, owner info
- Rich: images, reviews (text + rating + timestamp)
- Links: reservations, online ordering, menus

## Comparison

| Aspect | Serper API (current) | Browser Scraper |
|--------|---------------------|-----------------|
| Cost | $1/1k queries | Free |
| Speed | Fast (API) | Slow (browser) |
| Data fields | ~15 | 33+ |
| Reviews | No | Yes (~300/place) |
| Reliability | High | Medium (can break) |
| Setup | Easy (API key) | Complex (browser) |
| Rate limits | 5-50 qps | Unlimited w/ proxies |

## Integration Options

### Option 1: Replace Serper entirely
- Rewrite grid_scraper.py to use Playwright
- More data, free, but slower and more complex

### Option 2: Hybrid approach
- Use Serper for initial discovery (fast, cheap)
- Use browser scraper for enrichment (reviews, details)
- Best of both worlds

### Option 3: Keep as reference only
- Stick with Serper for simplicity
- Reference browser scraper techniques if needed later

## Key Files in Reference Project
```
google-maps-scraper/
├── gmaps/
│   ├── entry.go        # Data model (33+ fields)
│   ├── searchjob.go    # HTTP-based search
│   ├── job.go          # Browser automation
│   ├── place.go        # JS state extraction
│   └── reviews.go      # Review fetching (RPC + DOM)
├── runner/
│   ├── filerunner/     # CLI mode
│   ├── webrunner/      # Web UI mode
│   └── databaserunner/ # Distributed mode
└── web/                # REST API
```

## Useful Patterns to Steal

### 1. Protocol Buffer params for Google Maps
```go
pb := fmt.Sprintf("!4m12!1m3!1d3826.9!2d%.4f!3d%.4f!2m3!1f0!2f0!3f0...", lon, lat)
```

### 2. Review fetching via RPC
- Reverse-engineered Google's internal API
- Paginated with nextPageToken
- Falls back to DOM parsing if RPC fails

### 3. Distributed scraping with PostgreSQL
- Jobs stored in database
- Multiple workers consume from same queue
- Built-in deduplication

## Current Serper Performance (Florida Test - Jan 2026)

| Metric | Value | Notes |
|--------|-------|-------|
| Hotels scraped | 3,506 | Miami, Orlando, Tampa |
| API calls | ~4,200 | $4.20 cost |
| Non-hotels filtered | 1,089 (31%) | Restaurants, gas stations, etc. |
| No website | 591 (17%) | Can't detect booking engine |
| Engines detected | 449 (13%) | Have direct booking |
| Launched | 221+ | Fully enriched leads |

**Key Insight:** 31% of Serper results are garbage (non-hotels). Browser scraper could filter by Google place type before saving, reducing waste.

## Decision: Hybrid Approach (Recommended)

1. **Keep Serper for discovery** - Fast, simple, $0.01/detected engine
2. **Consider browser scraper for:**
   - Fetching reviews (for sentiment analysis)
   - Getting more fields (hours, price range)
   - Reducing 31% garbage rate via place type filtering

## Next Steps
- [x] Test Serper-based pipeline end-to-end ✓
- [x] Measure waste rate and detection rate ✓
- [ ] Evaluate if 31% waste justifies browser scraper complexity
- [ ] If yes: Prototype place type filtering with Playwright
- [ ] Consider browser scraper for review enrichment (post-detection)
