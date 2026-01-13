# Database Schema

## Overview

The Sadie GTM database has **7 tables** organized around a status-based pipeline:

```
Hotels → Detect Booking Engine → Enrich Data → Export to Sales
```

**Pipeline statuses:**
- `0` = scraped (new hotel)
- `1` = detected (booking engine found)
- `3` = enriched (has room count + customer proximity)
- `5` = live (ready for sales)
- `6` = exported (sent to sales team)
- `99` = no_booking_engine (dead end)

## Tables

### Core Tables

#### `hotels`
The main table. Every hotel goes through the pipeline.

```sql
id              SERIAL PRIMARY KEY
name            TEXT NOT NULL
website         TEXT

-- Contact
phone_google    TEXT
phone_website   TEXT
email           TEXT

-- Location (PostGIS)
location        GEOGRAPHY(POINT, 4326)  -- Lat/lng coordinates
address         TEXT
city            TEXT                     -- Not normalized, just text
state           TEXT
country         TEXT DEFAULT 'USA'

-- Ratings
rating          DOUBLE PRECISION
review_count    INTEGER

-- Pipeline
status          SMALLINT DEFAULT 0       -- Pipeline status

-- Metadata
source          TEXT                     -- grid, zipcode, serper
created_at      TIMESTAMP
updated_at      TIMESTAMP
```

**Key points:**
- City/state are TEXT fields (not foreign keys) - grid scraper returns city names, we store them as-is
- PostGIS `GEOGRAPHY(POINT, 4326)` for accurate distance calculations
- Unique constraint: `(name, COALESCE(website, ''))`

#### `booking_engines`
Reference table of known booking engines.

```sql
id              SERIAL PRIMARY KEY
name            TEXT NOT NULL UNIQUE
domains         TEXT[]                   -- Array of domains
tier            INTEGER DEFAULT 1        -- 1=known, 2=unknown
is_active       BOOLEAN DEFAULT TRUE
```

**Examples:** SynXis, Cloudbeds, Booking.com

#### `existing_customers`
Current Sadie customers (used for proximity enrichment).

```sql
id              SERIAL PRIMARY KEY
name            TEXT NOT NULL
sadie_hotel_id  TEXT

-- Location
location        GEOGRAPHY(POINT, 4326)
address         TEXT
city            TEXT
state           TEXT
country         TEXT DEFAULT 'USA'

-- Status
status          TEXT DEFAULT 'active'    -- active, churned, trial
go_live_date    DATE

created_at      TIMESTAMP
```

### Junction/Enrichment Tables

#### `hotel_booking_engines`
Links hotels to their detected booking engine.

```sql
hotel_id            INTEGER PRIMARY KEY → hotels(id)
booking_engine_id   INTEGER → booking_engines(id)

booking_url         TEXT
detection_method    TEXT                 -- playwright, regex, manual

detected_at         TIMESTAMP
updated_at          TIMESTAMP
```

**Key point:** Only created for hotels WITH booking engines (status=1). Hotels with status=99 have no row here.

#### `hotel_room_count`
Room count enrichment data.

```sql
id              SERIAL PRIMARY KEY
hotel_id        INTEGER UNIQUE → hotels(id)
room_count      INTEGER NOT NULL
source          TEXT                     -- groq, google, manual
confidence      DECIMAL(3,2)             -- 0.00 to 1.00
enriched_at     TIMESTAMP
```

#### `hotel_customer_proximity`
Distance to nearest existing Sadie customer.

```sql
id                      SERIAL PRIMARY KEY
hotel_id                INTEGER UNIQUE → hotels(id)
existing_customer_id    INTEGER → existing_customers(id)
distance_km             DECIMAL(6,1)     -- Distance in kilometers
computed_at             TIMESTAMP
```

**Key point:** Distance stored as DECIMAL, not geography type. Calculated with `ST_Distance()` and stored.

#### `jobs`
Job execution tracking for async workers.

```sql
id              SERIAL PRIMARY KEY

-- Job identification
job_type        TEXT NOT NULL            -- scrape, detect, enrich-room, export, etc.
hotel_id        INTEGER → hotels(id)

-- For export jobs
city            TEXT
state           TEXT
export_type     TEXT                     -- city, state

-- Queue info
queue_name      TEXT
message_id      TEXT                     -- SQS message ID
attempt_number  INTEGER DEFAULT 1

-- Execution
worker_id       TEXT                     -- hostname or container ID
started_at      TIMESTAMP
completed_at    TIMESTAMP
duration_ms     INTEGER

-- Status
status          SMALLINT DEFAULT 1       -- 0=pending, 1=running, 2=completed, 3=failed, 4=retrying
error_message   TEXT
error_stack     TEXT

-- Metadata
input_params    JSONB                    -- Job parameters
output_data     JSONB                    -- Job results
s3_log_path     TEXT                     -- S3 log file location
```

## PostGIS Usage

### Storing Coordinates

```sql
-- Insert with ST_Point(longitude, latitude)
INSERT INTO hotels (name, location)
VALUES ('Hotel Name', ST_Point(-80.1918, 25.7617)::geography);
```

**Order:** `ST_Point(longitude, latitude)` - longitude first!

### Querying Coordinates

```sql
-- Get lat/lng from geography
SELECT
    ST_Y(location::geometry) AS latitude,
    ST_X(location::geometry) AS longitude
FROM hotels;
```

### Calculating Distance

```sql
-- Distance in kilometers
SELECT ST_Distance(
    hotel.location,
    customer.location
) / 1000 AS distance_km
FROM hotels hotel, existing_customers customer;
```

**Returns meters by default, divide by 1000 for kilometers.**

## Design Decisions

### Why TEXT for city/state instead of foreign keys?

1. **Grid scraper returns city names** - Google Maps API gives us "Miami Beach", we store it as-is
2. **City boundaries are fuzzy** - One hotel can be in "Miami" or "Miami Beach" depending on the source
3. **No validation needed** - We trust Google's data, just store and display it
4. **Simpler queries** - No joins needed for city/state columns

### Why separate enrichment tables?

1. **Not all hotels get enriched** - Only status=1+ hotels get room counts
2. **Independent processes** - Room count and proximity can be enriched separately
3. **Clear data ownership** - Each enrichment has its own source, confidence, timestamp

### Why no views?

Keep it simple. Queries are in `/db/queries.sql` for analytics, but no materialized/regular views in schema.

### Why status-based pipeline?

1. **Clear progression** - Each status represents a stage: scraped → detected → enriched → live → exported
2. **Easy filtering** - `WHERE status = 0` gets all hotels pending detection
3. **Queue workers** - Workers poll for hotels at specific status levels

## Schema Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                           hotels                            │
│  id, name, website, city, state, location, status, ...     │
└────────────┬────────────────────────────────────────────────┘
             │
             ├─→ hotel_booking_engines → booking_engines
             │   (detection data)
             │
             ├─→ hotel_room_count
             │   (room count enrichment)
             │
             ├─→ hotel_customer_proximity → existing_customers
             │   (distance to nearest customer)
             │
             └─→ jobs
                 (work tracking)
```

## Migration Strategy

We use **migra** (not Alembic) for schema migrations.

```bash
# Generate migration from local to prod
migra postgresql://local/db postgresql://prod/db --unsafe > migration.sql

# Review and apply
psql -U user -d prod -f migration.sql
```

See `/context/docs/README_MIGRATIONS.md` for details.
