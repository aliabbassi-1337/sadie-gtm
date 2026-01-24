# Task: Government Hotel Data Enrichment

## Overview

Enrich government parcel/tax data (SF, Maryland) with hotel names and websites using Serper Maps API.

**Input:** `s3://sadie-gtm/hotel-sources/us/california/san_francisco_hotels.csv`
**Output:** `s3://sadie-gtm/hotel-sources/us/california/san_francisco_hotels_enriched.csv`

## Problem

We have official government data with:
- Exact room counts
- Lat/lon coordinates
- Property addresses
- Parcel numbers

But NO hotel names or websites. Need to enrich via Serper Maps API.

## Data Available

### San Francisco (606 hotels)
```csv
parcel_number,address,lat,lon,property_type,rooms
0503030,"2775 VAN NESS AV, San Francisco, CA",37.800955,-122.424973,Motels,138
```

### Maryland (1,274 hotels)
- Has room counts but needs geocoding first (parcel data only)

## Enrichment Approach

### Serper Maps API
```
POST https://google.serper.dev/places
Headers:
  X-API-KEY: {SERPER_API_KEY}
  Content-Type: application/json

Body:
{
  "q": "hotel",
  "ll": "@{lat},{lon},17z"
}
```

Returns places near coordinates with name, address, website, phone, rating.

## Implementation

### Function: `enrich_hotel_from_coords(lat, lon, address)`

```python
async def enrich_hotel_from_coords(lat: float, lon: float, address: str) -> dict:
    """
    1. Serper Places search at lat/lon for "hotel"
    2. Find closest match to our coordinates
    3. Return enriched data
    """

    response = await httpx.post(
        "https://google.serper.dev/places",
        headers={
            "X-API-KEY": os.environ["SERPER_API_KEY"],
            "Content-Type": "application/json"
        },
        json={
            "q": "hotel",
            "ll": f"@{lat},{lon},17z"
        }
    )

    data = response.json()
    places = data.get("places", [])

    if not places:
        return {"matched": False}

    # Get closest result
    best = places[0]

    return {
        "matched": True,
        "name": best.get("title"),
        "website": best.get("website"),
        "phone": best.get("phoneNumber"),
        "rating": best.get("rating"),
        "cid": best.get("cid")
    }
```

### Batch Processing

```python
async def enrich_sf_hotels():
    """Process all SF hotels with rate limiting."""

    df = pd.read_csv("s3://sadie-gtm/hotel-sources/us/california/san_francisco_hotels.csv")

    enriched = []
    for _, row in df.iterrows():
        result = await enrich_hotel_from_coords(row.lat, row.lon, row.address)
        enriched.append({**row.to_dict(), **result})
        await asyncio.sleep(0.1)  # Rate limit

    pd.DataFrame(enriched).to_csv("san_francisco_hotels_enriched.csv", index=False)
```

## Output Schema

```csv
parcel_number,address,lat,lon,property_type,rooms,name,website,phone,rating,cid,matched
0503030,"2775 VAN NESS AV, San Francisco, CA",37.800955,-122.424973,Motels,138,Holiday Inn Express,https://www.ihg.com/...,(415) 555-1234,4.2,12345,True
```

## Edge Cases

1. **No match** - Flag for manual review, keep original data
2. **Multiple hotels same location** - Take closest match by distance
3. **No website** - Common for small motels, still capture name/phone

## Cost

Serper: $0.001 per request (1000x cheaper than Google Places)
- SF (606 hotels): ~$0.60
- Maryland (1,274 hotels): ~$1.30

## Files to Create

```
services/enrichment/
├── serper_client.py     # Serper API client
├── gov_data_enricher.py # Batch enrichment logic
└── gov_data_enricher_test.py
```

## Environment

```
SERPER_API_KEY=...
```

## Usage

```python
from services.enrichment.gov_data_enricher import enrich_sf_hotels

# Run enrichment
await enrich_sf_hotels()

# Upload to S3
aws s3 cp san_francisco_hotels_enriched.csv s3://sadie-gtm/hotel-sources/us/california/
```
