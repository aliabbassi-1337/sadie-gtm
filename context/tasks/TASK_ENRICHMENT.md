# Task: Enrichment Service

## Overview

Implement the enrichment service in `/services/enrichment/`. The enrichment service adds room counts and customer proximity data to hotels that have completed detection (status=1).

**Location:** `/services/enrichment/`

## Service Functions

The service needs **3 functions**:

### 1. `enrich_room_counts(limit: int = 100) -> int`

Get room counts for hotels with status=1 (detected).

**Logic:**
1. Query hotels where `status = 1` and no entry in `hotel_room_count`
2. For each hotel with a website:
   - Fetch homepage + room-related pages (about, rooms, accommodations)
   - Try regex extraction first (fast, accurate)
   - Fall back to LLM estimation (Groq or Google AI)
3. Insert result into `hotel_room_count` table

**Room Count Extraction:**
- Regex patterns: `(\d+) rooms`, `(\d+)-room hotel`, `featuring (\d+) suites`
- LLM prompt asks for estimate based on property type
- Sanity check: 1-2000 rooms

**Returns:** Number of hotels enriched

### 2. `calculate_customer_proximity(limit: int = 100) -> int`

Calculate distance to nearest existing Sadie customer.

**Logic:**
1. Query hotels where `status = 1` and no entry in `hotel_customer_proximity`
2. For each hotel with lat/lng:
   - Use PostGIS `ST_Distance()` to find nearest `existing_customer`
   - Only consider customers within 100km
3. Insert result into `hotel_customer_proximity` table

**PostGIS Query:**
```sql
SELECT c.id, ST_Distance(h.location, c.location) / 1000 AS distance_km
FROM existing_customers c
WHERE ST_DWithin(h.location, c.location, 100000)  -- 100km
ORDER BY ST_Distance(h.location, c.location)
LIMIT 1;
```

**Returns:** Number of hotels processed

### 3. `get_pending_enrichment_count() -> int`

Count hotels waiting for enrichment.

**Query:** `SELECT COUNT(*) FROM hotels WHERE status = 1`

## Database Updates

### Room Count Enrichment

Insert into `hotel_room_count`:
```sql
INSERT INTO hotel_room_count (hotel_id, room_count, source, confidence, enriched_at)
VALUES (:hotel_id, :room_count, :source, :confidence, NOW());
```

- `source`: "regex", "groq", "google_ai"
- `confidence`: 1.0 for regex, 0.7 for LLM

### Customer Proximity

Insert into `hotel_customer_proximity`:
```sql
INSERT INTO hotel_customer_proximity (hotel_id, existing_customer_id, distance_km, computed_at)
VALUES (:hotel_id, :customer_id, :distance_km, NOW());
```

### Status Update

After BOTH enrichments complete, update hotel status:
```sql
UPDATE hotels SET status = 3, updated_at = NOW() WHERE id = :hotel_id;
```

Status 3 = enriched (ready for review)

## Implementation Notes

- Room count uses httpx for async HTTP
- LLM fallback uses Groq API (fast) or Google AI (fallback)
- API keys from env: `ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY`, `GOOGLE_AI_API_KEY`
- Rate limiting: Groq has 30 RPM limit, add 2.5s delay between calls
- Proximity uses PostGIS functions (already have coordinates in DB)

## Existing Scripts

Reference implementations in `/scripts/enrichers/`:
- `room_count_groq.py` - LLM-powered room count extraction
- `room_count_google.py` - Google AI fallback
- `customer_match.py` - Customer proximity calculation

## Files to Create/Modify

```
services/enrichment/
├── service.py          # Implement the 3 functions
├── repo.py             # NEW - Database queries
├── room_enricher.py    # NEW - Room count extraction logic
└── service_test.py     # Add tests
```

## Example Usage

```python
from services.enrichment import service

svc = service.Service()

# Check pending
pending = await svc.get_pending_enrichment_count()
print(f"{pending} hotels need enrichment")

# Enrich room counts
enriched = await svc.enrich_room_counts(limit=50)

# Calculate proximity
processed = await svc.calculate_customer_proximity(limit=50)
```
