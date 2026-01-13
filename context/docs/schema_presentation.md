# Sadie GTM Database Schema

## Architecture

```mermaid
flowchart LR
    subgraph Scraping
        OSM[OSM Scraper]
        SERPER[Serper Scraper]
        GRID[Grid Scraper]
    end

    subgraph Storage
        CSV[(CSV Files)]
        DB[(Supabase PostgreSQL)]
    end

    subgraph Pipeline
        INGEST[Ingest]
        DETECT[Detect]
        ENRICH[Enrich]
    end

    OSM --> CSV
    SERPER --> CSV
    GRID --> CSV
    CSV --> INGEST
    INGEST --> DB
    DB --> DETECT
    DETECT --> DB
    DB --> ENRICH
    ENRICH --> DB
```

---

## Data Model

```mermaid
erDiagram
    hotels ||--o| leads : "becomes"
    leads ||--o| lead_booking_engine : "has"
    leads ||--o| lead_location : "has"
    leads ||--o| lead_room_count : "has"
    leads ||--o| lead_contact : "has"
    leads ||--o| lead_research : "has"
    leads ||--o| lead_customer_proximity : "has"
    leads ||--o| lead_score : "has"
    lead_booking_engine }o--|| booking_engines : "FK"
    lead_customer_proximity }o--|| existing_customers : "FK"
    existing_customers ||--o| existing_customer_location : "has"

    hotels {
        bigint hotel_id PK
        text name
        text domain
        text city
        text state
    }

    leads {
        bigint lead_id PK
        bigint hotel_id FK
        text status
    }

    lead_booking_engine {
        bigint lead_id FK
        int booking_engine_id FK
        text booking_url
    }

    booking_engines {
        int booking_engine_id PK
        text name
        int tier
    }
```

---

## Tables Overview

| Table | Purpose |
|-------|---------|
| `hotels` | Raw scraped data (source of truth) |
| `leads` | Hotels that passed detection |
| `lead_booking_engine` | FK to booking_engines reference |
| `lead_location` | Enriched lat/long coordinates |
| `lead_room_count` | Room count + source + confidence |
| `lead_contact` | Phone, email (normalized) |
| `lead_research` | AI agent research (JSONB) |
| `lead_customer_proximity` | FK to nearest existing customer |
| `lead_score` | Scoring and prioritization |
| `booking_engines` | Reference: known engines + tier |
| `existing_customers` | Sadie customers |
| `existing_customer_location` | Customer coordinates |
| `pipeline_runs` | Execution tracking |

---

## Lead Extension Tables

```
leads
    ├── lead_booking_engine       → FK to booking_engines
    ├── lead_location             → lat/long + source
    ├── lead_room_count           → count + source + confidence
    ├── lead_contact              → phone, email
    ├── lead_research             → AI research (JSONB)
    ├── lead_customer_proximity   → FK to existing_customers
    └── lead_score                → scoring (0-100)
```

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Dedupe hotels | `UNIQUE(name, domain)` | Same name + domain = same hotel |
| Dedupe leads | `UNIQUE(hotel_id)` | One lead per hotel |
| Booking engine | FK to reference table | Consistent tier classification |
| Location | Separate table | Independent enrichment, source tracking |
| Research | JSONB | Flexible AI output, multiple types |
| Coordinates | Copied to lead_location | Leads self-contained, no join to hotels |

---

## Data Flow

```mermaid
flowchart TD
    CSV[CSV Files] -->|ingest.py| HOTELS[(hotels)]
    HOTELS -->|detect.py| LEADS[(leads)]
    LEADS --> LBE[lead_booking_engine]
    LEADS --> LL[lead_location]
    LEADS --> LRC[lead_room_count]
    LEADS --> LC[lead_contact]
    LEADS --> LCP[lead_customer_proximity]
    LEADS --> LS[lead_score]

    LBE -.->|FK| BE[(booking_engines)]
    LCP -.->|FK| EC[(existing_customers)]
```

---

## Costs

| Item | Cost |
|------|------|
| Supabase Free Tier | $0 (500MB DB, 1GB storage) |
| Scraping (Serper) | Already paid during scrape |
| Geocoding missing leads | ~$0.19 for 186 leads |
| Re-geocoding | Optional, ~$0.001/lead |

---

## Migration Plan

1. Create Supabase project
2. Run `schema.sql`
3. Build `ingest.py` to load CSVs → hotels
4. Update `detect.py` to write → leads + lead_booking_engine
5. Update enrichers to write → lead_* tables
6. Keep CSV backup for 2 weeks
7. Deprecate CSV workflow

---

## Code Architecture

```mermaid
flowchart TD
    subgraph Services["Services (decoupled, DB access only)"]
        LGS[Lead Generation Service]
        SES[Sales Enablement Service]
    end

    subgraph Workflows["Workflows (Luigi DAGs)"]
        W1[Scrape Workflow]
        W2[Detect Workflow]
        W3[Enrich Workflow]
        W4[Export Workflow]
    end

    subgraph Analytics["Analytics (DuckDB)"]
        SCORE[Lead Scoring]
    end

    DB[(Supabase)]

    W1 -->|inject| LGS
    W2 -->|inject| LGS
    W3 -->|inject| LGS
    W4 -->|inject| SES

    LGS -->|interface| DB
    SES -->|interface| DB

    DB --> SCORE
```

### Services

- **Completely decoupled** - no intercommunication between services
- Each service has its own repo(s)
- **Interface** - functions that can be used externally by workflows or APIs
- **Only way to access DB** is through a service function

### Workflows

- Tasks/jobs that **dependency inject** the service
- Simple orchestration, outsource work to service
- **Luigi** for task dependencies and DAGs

### Producers

- Each producer has a **dataloader**
- DAG organizes running multiple producers
- Scheduled or triggered

### Analytics

- **DuckDB** for analytics jobs (lead scoring, etc.)
- Last feature to build
