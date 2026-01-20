# Task: Florida State Business License Database

## Goal
Extract hotel/lodging leads from Florida's official business license database (DBPR - Department of Business and Professional Regulation).

## Why This Works
- **Complete coverage**: Every legal lodging business MUST be licensed
- **Pre-filtered**: License categories identify hotels/motels specifically
- **Accurate data**: Official government records with addresses, owners
- **No duplicates**: Unique license numbers
- **Free**: Public records

## Data Source

### Florida DBPR (Department of Business and Professional Regulation)
- **Website**: https://www.myfloridalicense.com
- **License Search**: https://www.myfloridalicense.com/wl11.asp
- **Data Portal**: May have bulk download or API

### License Types to Target
- `Hotel` - Traditional hotels
- `Motel` - Motels
- `Bed and Breakfast` - B&Bs
- `Vacation Rental` - Short-term rentals (if licensed)
- `Transient Apartment` - Extended stay
- `Rooming House` - Boarding houses
- `Resort` - Resort properties

## Data Fields Available
- License Number
- Business Name (DBA)
- Owner/Licensee Name
- Address (physical)
- Mailing Address
- License Status (Active/Inactive)
- License Issue Date
- Expiration Date
- County
- Phone (sometimes)

## Approach 1: Web Scraping

```python
# Scrape the license search portal
# https://www.myfloridalicense.com/wl11.asp

# 1. Select license type (Hotel/Motel)
# 2. Select county or search all
# 3. Parse results table
# 4. Handle pagination
```

**Pros**: Free, comprehensive
**Cons**: May need to handle anti-bot measures, slow

## Approach 2: Public Records Request

Florida has strong public records laws (Sunshine Law). Can request bulk data export.

```
Request to: DBPR Public Records
Data: All active lodging licenses in Florida
Format: CSV/Excel preferred
```

**Pros**: Clean official data, complete
**Cons**: May take days/weeks, possible fees

## Approach 3: Open Data Portal

Check if Florida has open data portal with license data:
- https://data.florida.gov
- https://geodata.myflorida.com

Some states publish license data as downloadable datasets.

## Approach 4: Third-Party Data Providers

Companies that aggregate business license data:
- Data.com
- InfoUSA/Infogroup
- Dun & Bradstreet
- State-specific data vendors

**Cons**: Paid, may not be current

## Implementation Plan

### Phase 1: Research & Validate
1. Explore DBPR website structure
2. Test manual searches
3. Check for API or bulk download options
4. Evaluate scraping feasibility

### Phase 2: Scraper Development
```python
# services/leadgen/dbpr_scraper.py

class DBPRScraper:
    BASE_URL = "https://www.myfloridalicense.com"

    async def search_licenses(
        self,
        license_type: str,  # "Hotel", "Motel", etc.
        county: str = None,
        status: str = "Active",
    ) -> List[License]:
        pass

    async def scrape_all_lodging(self) -> List[License]:
        # Iterate through all lodging types
        # Handle pagination
        # Dedupe by license number
        pass
```

### Phase 3: Data Enrichment
1. Match licenses to existing hotels (by address/name)
2. Find new hotels not in our database
3. Use address to geocode (lat/lng)
4. Scrape website from Google if not in license data

### Phase 4: Integration
1. Add to hotel pipeline
2. Run booking engine detection on new leads
3. Track source as "dbpr_license"

## Expected Results

| County | Est. Lodging Licenses |
|--------|----------------------|
| Miami-Dade | 500-1000 |
| Broward | 300-500 |
| Palm Beach | 200-400 |
| Orange (Orlando) | 400-600 |
| Hillsborough (Tampa) | 200-400 |
| Duval (Jacksonville) | 100-200 |
| **Total Florida** | **3000-5000+** |

## Data Quality Notes

- **Active vs Inactive**: Only scrape active licenses
- **Address variations**: Same hotel may have different address formats
- **Name matching**: "Beach Hotel LLC" vs "The Beach Hotel" - need fuzzy matching
- **No website**: Many licenses won't have website - need Google lookup

## Comparison to Google Maps

| Aspect | Google Maps | DBPR Licenses |
|--------|-------------|---------------|
| Coverage | Popular places | ALL licensed |
| Data quality | Variable | Official |
| Website included | Yes | Rarely |
| Phone included | Yes | Sometimes |
| Lat/Lng | Yes | No (need geocoding) |
| Cost | $16/region | Free |

## Files to Create

```
services/leadgen/
├── dbpr_scraper.py        # DBPR license scraper
└── license_enricher.py    # Enrich with website/coords

workflows/
└── scrape_dbpr.py         # CLI workflow
```

## Quick Test

```bash
# Manual test - search Palm Beach hotels
# 1. Go to https://www.myfloridalicense.com/wl11.asp
# 2. License Type: Hotel
# 3. County: Palm Beach
# 4. Status: Active
# 5. Count results
```

## Legal Considerations
- Public records - OK to scrape
- Rate limiting - Be respectful
- Terms of service - Review DBPR ToS
- Data usage - Commercial use typically OK for public records

## Next Steps
- [ ] Manual exploration of DBPR website
- [ ] Count total lodging licenses in Florida
- [ ] Check for bulk download/API options
- [ ] Submit public records request as backup
- [ ] Build scraper prototype
- [ ] Test on one county (Palm Beach)
