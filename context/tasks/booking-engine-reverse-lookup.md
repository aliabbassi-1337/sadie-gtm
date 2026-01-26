# Task: Booking Engine Reverse Lookup

## Goal
Find hotels by identifying websites using specific booking engine software (Cloudbeds, Guesty, Little Hotelier, etc.) instead of searching for hotels directly.

## Why This Works
- Booking engines have predictable URL patterns and footprints
- Hotels using these engines are exactly our target market (independent properties with direct booking)
- Bypasses Google Maps limitations entirely
- These are pre-qualified leads - they already have booking software

## Cloudbeds ID Structure (Research Findings - Jan 2026)

Cloudbeds uses two types of identifiers:

### 1. Public Slug (6 alphanumeric chars)
- Format: e.g., `cl6l0S`, `UxSswi`
- Used in booking URLs: `hotels.cloudbeds.com/reservation/{slug}`
- **Not a simple base62 encoding** of the numeric ID
- Likely hash-based or encrypted

### 2. Internal Numeric ID (Sequential)
- Format: e.g., `317832`, `202743`
- Currently ~300,000+ properties in Cloudbeds
- Exposed in:
  - Image URLs: `h-img*.cloudbeds.com/uploads/{property_id}/`
  - Analytics: `ep.property_id={property_id}`
  - Facebook tracking: `cd[property_id]={property_id}`
- **Cannot be used directly** in booking URLs

### Known Mappings

| Hotel                         | Slug    | Numeric ID | City        | State |
|-------------------------------|---------|------------|-------------|-------|
| The Kendall                   | cl6l0S  | 317832     | Boerne      | TX    |
| 7 Seas Hotel                  | UxSswi  | 202743     | Miami       | FL    |
| St Augustine Hotel            | sEhTC1  | -          | Miami Beach | FL    |
| Casa Ocean                    | TxCgVr  | -          | Miami Beach | FL    |
| Sebastian Gardens Inn & Suites| iocJE7  | -          | Sebastian   | FL    |
| Up Midtown                    | UpukGL  | -          | Miami       | FL    |

## Target Booking Engines

### Cloudbeds
- **Booking URLs**: `hotels.cloudbeds.com/reservation/{property-slug}`
- **Widget patterns**: Scripts loading from `static1.cloudbeds.com`
- **Google dork**: `site:hotels.cloudbeds.com florida`
- **TheGuestbook API**: Partner directory with 800+ Cloudbeds hotels

### Guesty
- **Booking URLs**: `*.guestybookings.com`
- **Google dork**: `site:guestybookings.com florida`

### Little Hotelier
- **Booking URLs**: `*.littlehotelier.com`
- **Widget patterns**: iframes from `app.littlehotelier.com`

### WebRezPro
- **Footprint**: `"powered by webrezpro"`

### Lodgify
- **Booking URLs**: `*.lodgify.com/booking`

### Hostaway
- **URLs**: `*.hostaway.com`

### Other Targets
- Cloudbeds, SiteMinder, RMS Cloud, innRoad, ResNexus
- Check BuiltWith for full list of hospitality tech

## Approach 1: Google Dorks

```python
# Search patterns
dorks = [
    'site:hotels.cloudbeds.com "florida"',
    'site:guestybookings.com florida hotel',
    '"powered by cloudbeds" florida',
    '"book direct" "cloudbeds" florida',
    'inurl:reservation cloudbeds florida',
]
```

**Pros**: Free, quick to test
**Cons**: Limited results, Google rate limits

## Approach 2: BuiltWith API

BuiltWith tracks technology usage across websites. Query for all sites using specific booking software in Florida.

```python
# BuiltWith API
# https://api.builtwith.com/v21/api.json?KEY=xxx&TECH=Cloudbeds&META=Florida
```

**Pricing**: ~$295/month for API access
**Data**: Technology profiles for millions of sites

## Approach 3: Certificate Transparency Logs

SSL certificates are public. Search for certs issued to booking engine subdomains.

```bash
# Search crt.sh for Cloudbeds subdomains
curl "https://crt.sh/?q=%.cloudbeds.com&output=json"
```

**Pros**: Free, comprehensive
**Cons**: Need to filter/dedupe, some false positives

## Approach 4: TheGuestbook API ⭐ NEW - IMPLEMENTED

TheGuestbook is Cloudbeds' rewards program that lists 800+ partner hotels.
It has a public JSON API that returns hotel data including:
- Hotel name, coordinates, website
- Integration status (`beiStatus: "automated"` = Cloudbeds)
- Review scores

**API Endpoint**: 
```
GET https://theguestbook.com/en/destinations/guestbook/fetch_properties
?check_in=2026-02-08&check_out=2026-02-11
&filters={"bbox":{"type":"Polygon","coordinates":[[...]]}}
&page=1&format=json
```

**Response**:
```json
{
  "results": {
    "15340": {
      "id": 15340,
      "name": "Glover Park Hotel Georgetown",
      "lat": "38.9233322",
      "lng": "-77.074961",
      "beiStatus": "automated",
      "trustYouScore": 8.8,
      "website": "https://..."
    }
  },
  "totalCount": 810,
  "totalPages": 41
}
```

**Pros**: Free, structured data, 800+ hotels, includes coordinates
**Cons**: Only Cloudbeds partners on TheGuestbook network

## Approach 5: Direct Subdomain Enumeration

Cloudbeds booking pages follow pattern: `hotels.cloudbeds.com/reservation/{slug}`

The slug is a 6-character alphanumeric code that cannot be enumerated sequentially
(it's not a simple base62 encoding of the property ID).

## Approach 6: Scrape Booking Engine Marketing

Some engines showcase customers:
- Case studies pages
- Customer testimonials
- "Powered by" footers on hotel sites

## Implementation Status

### ✅ Phase 1: Google Dorks (COMPLETE)
- Implemented in `services/leadgen/reverse_lookup.py`
- CLI: `uv run python -m workflows.reverse_lookup -l "Florida"`
- Supports 15+ booking engines

### ✅ Phase 2: TheGuestbook API (COMPLETE - Jan 2026)
- Implemented in `services/leadgen/booking_engines.py`
- CLI: `uv run python -m workflows.guestbook_enum --florida`
- Returns 800+ Cloudbeds partner hotels

### 🔄 Phase 3: BuiltWith Integration (PLANNED)
- Evaluate API pricing ($295/mo)
- Would give broader coverage

### 🔄 Phase 4: Certificate Transparency (PLANNED)
- Query crt.sh for booking engine subdomains
- Parse and filter results

## Expected Results

| Source | Est. New Leads | Cost | Effort | Status |
|--------|---------------|------|--------|--------|
| Google Dorks | 50-200 | Free (use existing Serper) | Low | ✅ Done |
| TheGuestbook | 800+ | Free | Low | ✅ Done |
| BuiltWith | 500-2000 | $295/mo | Medium | Planned |
| CT Logs | 100-500 | Free | Medium | Planned |

## Files

```
services/leadgen/
├── reverse_lookup.py      # Google dork patterns and models
├── booking_engines.py     # TheGuestbook scraper, Cloudbeds extractor
└── service.py             # reverse_lookup() method

workflows/
├── reverse_lookup.py      # Google dorks CLI
└── guestbook_enum.py      # TheGuestbook enumeration CLI
```

## Quick Test

```bash
# Test Google dork via Serper
uv run python -m workflows.reverse_lookup -l "Palm Beach Florida" --dry-run

# Test TheGuestbook API (fetch first 2 pages)
uv run python -m workflows.guestbook_enum --florida --max-pages 2
```

## Next Steps
- [x] Test Google dorks manually to validate results
- [x] Build dork scraper using existing Serper credits
- [x] Implement TheGuestbook API scraper
- [ ] Evaluate BuiltWith API trial
- [ ] Parse CT logs for Cloudbeds/Guesty subdomains
- [ ] Build Cloudbeds property detail extractor (with Playwright)
