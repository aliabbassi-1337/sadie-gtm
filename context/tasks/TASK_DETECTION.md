# Task: Detection Service

## Overview

Implement the detection service in the `leadgen` service. The detector visits hotel websites to identify their booking engine.

**Location:** `/services/leadgen/` (in the `sadie_gtm-detection` worktree)

## Service Functions

The service needs **2 functions**:

### 1. `get_hotels_pending_detection(limit: int) -> List[Hotel]`

Query hotels that need detection.

**Criteria:**
- `status = 0` (scraped)
- `website IS NOT NULL`
- Skip big chains (Marriott, Hilton, IHG, Hyatt, Wyndham)

**Returns:** List of Hotel models with `id`, `name`, `website`

### 2. `detect_booking_engines(hotels: List[Hotel]) -> List[DetectionResult]`

Detect booking engines for a list of hotels.

**Input per hotel:**
- `id` - to update DB record
- `name` - for logging
- `website` - the URL to visit

**Output per hotel:**
- `hotel_id`
- `booking_engine` - engine name (e.g., "Cloudbeds", "SynXis")
- `booking_engine_domain` - detected domain
- `booking_url` - the booking page URL
- `detection_method` - how it was detected (url_pattern, network_sniff, etc.)
- `phone_website` - phone extracted from website
- `email` - email extracted from website
- `error` - error message if failed

## Detection Logic

The existing script (`/scripts/pipeline/detect.py`) does:

1. **HTTP pre-check** - Skip unreachable URLs
2. **Visit website** with Playwright (headless Chrome)
3. **Find booking buttons** - Look for "Book Now", "Reserve", etc.
4. **Click and monitor network** - Watch for requests to booking engine domains
5. **Pattern match URLs** - Check if URLs contain known engine domains
6. **Extract contacts** - Phone, email from page HTML

### Known Booking Engines

The script has patterns for 100+ engines including:
- Cloudbeds, Mews, SynXis, WebRezPro, InnRoad, ResNexus
- SiteMinder, ThinkReservations, eZee, Lodgify, Guesty
- See `ENGINE_PATTERNS` dict in detect.py

### What to Skip

- Big chains: marriott.com, hilton.com, ihg.com, hyatt.com, wyndham.com
- Junk URLs: facebook.com, instagram.com, .gov, etc.

## Database Updates

After detection, update:

1. **hotels table:**
   - `status = 1` if booking engine found
   - `status = 99` if no booking engine
   - `phone_website`, `email` if extracted

2. **hotel_booking_engines table:**
   - Insert row linking hotel to booking_engine
   - Store `booking_url`, `detection_method`

3. **booking_engines table:**
   - Insert new engine if unknown (tier=2)

## Implementation Notes

- Use Playwright async API
- Run 5 concurrent workers (configurable)
- Timeout: 30s page load, 3s for button clicks
- Reuse browser contexts for efficiency

## Files to Create/Modify

```
services/leadgen/
├── service.py          # Add detect_booking_engines()
├── repo.py             # Add get_hotels_pending_detection(), update functions
├── detector.py         # NEW - Detection logic (extracted from script)
└── repo_test.py        # Add tests
```

## Example Usage

```python
from services.leadgen import service

svc = service.Service()

# Get hotels to process
hotels = await svc.get_hotels_pending_detection(limit=100)

# Detect engines
results = await svc.detect_booking_engines(hotels)

# Results automatically update DB
```
