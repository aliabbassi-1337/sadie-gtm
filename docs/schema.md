# Sadie GTM Database Schema

## Design Goals

1. **Deduplicate at insert** - No duplicate hotels or leads
2. **Track provenance** - Know where each record came from
3. **Pipeline state** - Track what's been processed
4. **Audit trail** - When was each record created/updated
5. **Fast queries** - Indexes for common access patterns

## Entity Relationship Diagram

```mermaid
erDiagram
    hotels ||--o| detections : "has"
    hotels ||--o| enrichments : "has"
    detections }o--|| booking_engines : "uses"

    hotels {
        bigint id PK
        text name "UK(name,domain)"
        text domain "UK(name,domain)"
        text place_id
        text address
        text city
        text state
        text country
        float latitude
        float longitude
        float rating
        int review_count
        text source
    }

    detections {
        bigint id PK
        bigint hotel_id FK_UK
        text booking_url
        text booking_engine
        int tier
        text status
    }

    enrichments {
        bigint id PK
        bigint hotel_id FK_UK
        text phone_google
        text phone_website
        text email
        int room_count
        text existing_customer
    }

    booking_engines {
        int id PK
        text name UK
        int tier
    }

    pipeline_runs {
        bigint id PK
        text run_type
        text state
        text city
        text status
    }
```

## Tables

### 1. `hotels` - Raw Scraped Data

Source of truth for all scraped hotel data.

```sql
CREATE TABLE hotels (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    domain          TEXT,                           -- normalized: "reallycoolhotel.com"
    place_id        TEXT,                           -- Google Place ID (optional)
    address         TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT DEFAULT 'USA',
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    rating          DECIMAL(2,1),                   -- e.g., 4.5
    review_count    INTEGER,
    source          TEXT,                           -- 'osm', 'serper', 'grid', 'zipcode'
    source_file     TEXT,                           -- original CSV filename
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Dedupe: same hotel name + domain = same hotel
    UNIQUE (name, domain)
);

-- Indexes
CREATE INDEX idx_hotels_city_state ON hotels(city, state);
CREATE INDEX idx_hotels_domain ON hotels(domain) WHERE domain IS NOT NULL;
CREATE INDEX idx_hotels_place_id ON hotels(place_id) WHERE place_id IS NOT NULL;
CREATE INDEX idx_hotels_location ON hotels USING GIST (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
) WHERE latitude IS NOT NULL;
```

**Dedupe strategy**: `(name, domain)` is unique. Hotels with the same name and domain are considered duplicates.

**Domain normalization**: Store only the domain, not full URL.
- `https://www.reallycoolhotel.com/booking?ref=google` → `reallycoolhotel.com`
- `http://the-grand-hotel.net` → `the-grand-hotel.net`

**Open questions:**
- Should `city` and `state` be normalized to separate tables?
- How to handle hotels without a domain? Allow `(name, NULL)` duplicates?

---

### 2. `detections` - Booking Engine Detection Results

Links to hotels, stores only detection-specific data.

```sql
CREATE TABLE detections (
    id                  BIGSERIAL PRIMARY KEY,
    hotel_id            BIGINT NOT NULL REFERENCES hotels(id) UNIQUE,

    -- Detection results
    booking_url         TEXT,
    booking_engine      TEXT,
    tier                SMALLINT,                   -- 1 = known, 2 = unknown

    -- Pipeline state
    status              TEXT DEFAULT 'detected',    -- detected, enriched, exported
    detected_at         TIMESTAMPTZ DEFAULT NOW(),
    enriched_at         TIMESTAMPTZ,
    exported_at         TIMESTAMPTZ,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_detections_booking_engine ON detections(booking_engine);
CREATE INDEX idx_detections_tier ON detections(tier);
CREATE INDEX idx_detections_status ON detections(status);
```

**Dedupe strategy**: `hotel_id` is unique. One detection per hotel.

---

### 3. `enrichments` - Additional Hotel Data

Room count, contact info, etc. gathered after detection.

```sql
CREATE TABLE enrichments (
    id                  BIGSERIAL PRIMARY KEY,
    hotel_id            BIGINT NOT NULL REFERENCES hotels(id) UNIQUE,

    -- Contact info
    phone_google        TEXT,
    phone_website       TEXT,
    email               TEXT,

    -- Room data
    room_count          INTEGER,

    -- Customer proximity
    existing_customer   TEXT,                       -- nearby Sadie customer name
    customer_distance   DECIMAL(5,1),               -- km to nearest customer

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Index for finding hotels needing enrichment
CREATE INDEX idx_enrichments_room_count ON enrichments(room_count) WHERE room_count IS NULL;
```

**Dedupe strategy**: `hotel_id` is unique. One enrichment record per hotel.

---

### 3. `booking_engines` - Reference Table

Known booking engines and their classification.

```sql
CREATE TABLE booking_engines (
    id          SERIAL PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    tier        SMALLINT NOT NULL DEFAULT 2,        -- 1 = target, 2 = other
    category    TEXT,                               -- 'pms', 'channel_manager', 'booking_widget'
    notes       TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed data
INSERT INTO booking_engines (name, tier, category) VALUES
    ('SynXis / TravelClick', 1, 'crs'),
    ('SiteMinder', 1, 'channel_manager'),
    ('Cloudbeds', 1, 'pms'),
    ('WebRezPro', 1, 'pms'),
    ('innRoad', 1, 'pms'),
    ('RoomRaccoon', 1, 'pms'),
    ('Beds24', 1, 'channel_manager'),
    ('Booking.com Widget', 2, 'ota_widget'),
    ('Expedia Widget', 2, 'ota_widget'),
    ('Unknown', 2, 'unknown');
```

**Open questions:**
- Should `leads.booking_engine` be a FK to this table, or free text?
- What other engines should be Tier 1?

---

### 4. `pipeline_runs` - Execution Tracking

Track each pipeline run for debugging and monitoring.

```sql
CREATE TABLE pipeline_runs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type            TEXT NOT NULL,              -- 'scrape', 'detect', 'enrich_room', 'enrich_customer', 'export'
    state               TEXT,                       -- 'florida', 'california', etc.
    city                TEXT,                       -- specific city or NULL for state-wide

    status              TEXT DEFAULT 'running',     -- running, completed, failed
    records_input       INTEGER,
    records_processed   INTEGER,
    records_skipped     INTEGER,
    records_failed      INTEGER,

    error_message       TEXT,

    started_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,

    -- Metadata
    script_name         TEXT,                       -- e.g., 'room_count_groq.py'
    parameters          JSONB                       -- any runtime params
);

CREATE INDEX idx_pipeline_runs_type_state ON pipeline_runs(run_type, state);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);
```

---

### 5. `existing_customers` - Sadie Customers

For customer proximity enrichment.

```sql
CREATE TABLE existing_customers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    hotel_id        TEXT,                           -- Sadie hotel ID
    location        TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    state           TEXT,
    country         TEXT,
    status          TEXT,                           -- 'active', 'churned', etc.
    go_live_date    DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_existing_customers_location ON existing_customers USING GIST (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
) WHERE latitude IS NOT NULL;
```

---

## Constraints Summary

| Table | Unique Constraint | Rationale |
|-------|------------------|-----------|
| hotels | `(name, domain)` | Same hotel name + domain = same hotel |
| leads | `domain` | One lead per hotel domain |
| booking_engines | `name` | Reference lookup |

## Status Enums

Consider using PostgreSQL enums for type safety:

```sql
CREATE TYPE lead_status AS ENUM ('detected', 'enriching', 'enriched', 'exported', 'failed');
CREATE TYPE pipeline_status AS ENUM ('running', 'completed', 'failed', 'cancelled');
CREATE TYPE tier_level AS ENUM ('1', '2');
```

**Open question:** Enums vs text columns? Enums are safer but harder to modify.

---

## Views

### Useful query views

```sql
-- Leads needing room count enrichment
CREATE VIEW v_needs_room_count AS
SELECT * FROM leads
WHERE room_count IS NULL AND status = 'detected';

-- State summary stats
CREATE VIEW v_state_stats AS
SELECT
    state,
    COUNT(*) as total_leads,
    COUNT(*) FILTER (WHERE tier = 1) as tier1,
    COUNT(*) FILTER (WHERE tier = 2) as tier2,
    COUNT(room_count) as with_room_count,
    COUNT(existing_customer) as with_customer
FROM leads
GROUP BY state;

-- Florida Top 25 cities
CREATE VIEW v_florida_top25 AS
SELECT * FROM leads
WHERE state = 'Florida'
AND LOWER(city) IN (
    'miami beach', 'kissimmee', 'miami', 'pensacola', 'fort lauderdale',
    'tampa', 'saint augustine', 'st augustine', 'key west', 'windermere',
    'panama city beach', 'bay pines', 'orlando', 'daytona beach',
    'north miami beach', 'pompano beach', 'homestead', 'fort myers beach',
    'hialeah', 'saint petersburg', 'st petersburg', 'clearwater beach',
    'jacksonville', 'sarasota', 'pembroke pines', 'fort myers', 'high springs'
);
```

---

## Open Questions for Discussion

1. **Hotels without domain**: How to handle? Allow multiple `(name, NULL)` entries, or require domain?

2. **hotel_id in leads**: Required FK or optional? Some leads might come from detection without matching hotel record.

3. **Denormalization**: Leads table has `city`, `state`, `latitude`, `longitude` copied from hotels. Worth it for query speed?

4. **Booking engine normalization**: Should `leads.booking_engine` FK to `booking_engines` table, or stay as free text for flexibility?

5. **Tier derivation**: Store `tier` explicitly, or derive from `booking_engine` at query time?

6. **Enums vs text**: Use PostgreSQL enums for status fields, or keep flexible text?

7. **Geospatial**: Use PostGIS for location queries, or just store lat/lon as floats?

8. **Soft deletes**: Add `deleted_at` column, or hard delete records?
