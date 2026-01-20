# Task: Booking Engine Reverse Lookup

## Goal
Find hotels by identifying websites using specific booking engine software (Cloudbeds, Guesty, Little Hotelier, etc.) instead of searching for hotels directly.

## Why This Works
- Booking engines have predictable URL patterns and footprints
- Hotels using these engines are exactly our target market (independent properties with direct booking)
- Bypasses Google Maps limitations entirely
- These are pre-qualified leads - they already have booking software

## Target Booking Engines

### Cloudbeds
- **Booking URLs**: `hotels.cloudbeds.com/reservation/{property-slug}`
- **Widget patterns**: Scripts loading from `static1.cloudbeds.com`
- **Google dork**: `site:hotels.cloudbeds.com florida`

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

## Approach 4: Direct Subdomain Enumeration

Cloudbeds booking pages follow pattern: `hotels.cloudbeds.com/reservation/{slug}`

```python
# Could potentially enumerate or find sitemap
# Some booking engines have public property directories
```

## Approach 5: Scrape Booking Engine Marketing

Some engines showcase customers:
- Case studies pages
- Customer testimonials
- "Powered by" footers on hotel sites

## Implementation Plan

### Phase 1: Google Dorks (Quick Win)
1. Build list of dorks for each booking engine
2. Use Serper or SerpAPI to run searches
3. Extract hotel URLs from results
4. Dedupe against existing database

### Phase 2: BuiltWith Integration
1. Evaluate BuiltWith API pricing
2. Query for hospitality tech in Florida
3. Cross-reference with our detected engines
4. Find hotels we missed

### Phase 3: Certificate Transparency
1. Query crt.sh for booking engine subdomains
2. Parse and filter results
3. Extract property names from subdomain patterns
4. Validate and enrich

## Expected Results

| Source | Est. New Leads | Cost | Effort |
|--------|---------------|------|--------|
| Google Dorks | 50-200 | Free (use existing Serper) | Low |
| BuiltWith | 500-2000 | $295/mo | Medium |
| CT Logs | 100-500 | Free | Medium |

## Files to Create

```
services/leadgen/
├── reverse_lookup.py      # Main reverse lookup logic
├── dorks.py               # Google dork patterns
└── builtwith.py           # BuiltWith API client

workflows/
└── reverse_lookup.py      # CLI workflow
```

## Quick Test

Run this manually to validate approach:
```bash
# Test Google dork via Serper
curl -X POST 'https://google.serper.dev/search' \
  -H 'X-API-KEY: YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"q": "site:hotels.cloudbeds.com florida"}'
```

## Next Steps
- [ ] Test Google dorks manually to validate results
- [ ] Build dork scraper using existing Serper credits
- [ ] Evaluate BuiltWith API trial
- [ ] Parse CT logs for Cloudbeds/Guesty subdomains
