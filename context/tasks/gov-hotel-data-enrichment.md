# Task: Government Hotel Data Enrichment

## Overview

Enrich government parcel/tax data (SF, Maryland) with hotel names and websites using Serper Maps API.

**Input:** `s3://sadie-gtm/hotel-sources/us/california/san_francisco_hotels.csv`

## Data Sources

### San Francisco Assessor Data
- **Main Dataset:** https://data.sfgov.org/Housing-and-Buildings/Assessor-Historical-Secured-Property-Tax-Rolls/wv5m-vpq2
- **Property Class Codes:** https://data.sfgov.org/Housing-and-Buildings/Reference-Assessor-Recorder-Property-Class-Codes/pa56-ek2h
- **API Endpoint:** `https://data.sfgov.org/resource/wv5m-vpq2.csv`
- **GeoJSON Endpoint:** `https://data.sfgov.org/resource/wv5m-vpq2.geojson`

### Maryland CAMA/SDAT Data
- **Download Page:** https://planning.maryland.gov/Pages/OurProducts/DownloadFiles.aspx
- **Notes:** Parcel data with room counts but no hotel names. Needs geocoding before enrichment.

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
├── web_enricher_with_{idk "location" maybe?}.py 
```

## Environment

```
SERPER_API_KEY=...
```
