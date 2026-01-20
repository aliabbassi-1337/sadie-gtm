# External Integrations

**Analysis Date:** 2026-01-20

## APIs & External Services

**Google Maps (via Serper):**
- Purpose: Hotel discovery via Maps search
- SDK/Client: Direct HTTP via `httpx`
- Endpoint: `https://google.serper.dev/maps`
- Auth: `SERPER_API_KEY` (header: `X-API-KEY`)
- Usage: `services/leadgen/grid_scraper.py`
- Cost: $0.001 per query (~$1 per 1000 searches)

**Groq LLM API:**
- Purpose: Room count estimation from website text
- SDK/Client: Direct HTTP via `httpx`
- Endpoint: `https://api.groq.com/openai/v1/chat/completions`
- Auth: `ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY` (Bearer token)
- Model: `llama-3.1-8b-instant`
- Usage: `services/enrichment/room_count_enricher.py`

**OpenStreetMap Nominatim:**
- Purpose: City geocoding and boundary polygons
- SDK/Client: Direct HTTP via `httpx`
- Endpoint: `https://nominatim.openstreetmap.org/search`
- Auth: None (free, rate-limited to 1 req/sec)
- Usage: `services/leadgen/geocoding.py`
- Features: Forward geocoding, reverse geocoding, polygon boundaries

**Slack Webhooks:**
- Purpose: Pipeline notifications
- SDK/Client: Direct HTTP via `httpx`
- Auth: `SLACK_WEBHOOK_URL`, `SLACK_LEADS_WEBHOOK_URL`
- Usage: `infra/slack.py`

## Data Storage

**PostgreSQL (PostGIS):**
- Provider: Local Docker (dev) / Supabase (prod)
- Image: `postgis/postgis:17-3.5-alpine`
- Connection:
  - `SADIE_DB_HOST`, `SADIE_DB_PORT`, `SADIE_DB_NAME`
  - `SADIE_DB_USER`, `SADIE_DB_PASSWORD`
- Client: `asyncpg` (async), `psycopg2` (sync for migra)
- Schema: `sadie_gtm` (custom search_path)
- Features: PostGIS spatial queries for proximity calculations
- Usage: `db/client.py`

**AWS S3:**
- Purpose: Excel report storage
- Bucket: `S3_BUCKET_NAME` (default: `sadie-gtm`)
- Client: `boto3`
- Path pattern: `HotelLeadGen/{country}/{state}/{city}.xlsx`
- Usage: `infra/s3.py`

**File Storage:**
- Local filesystem for temp Excel generation
- Uses Python `tempfile` module

**Caching:**
- None (database serves as cache for geocoding results)

## Message Queue

**AWS SQS:**
- Purpose: Detection job queue for EC2 workers
- Queue: `SQS_DETECTION_QUEUE_URL`
- Client: `boto3`
- Message format: `{"hotel_ids": [1, 2, 3, ...]}`
- Batch size: 20 hotels per message
- Visibility timeout: 7200s (2 hours)
- Usage: `infra/sqs.py`

**Operations:**
- `send_message()` - Single message
- `send_messages_batch()` - Up to 10 messages (SQS limit)
- `receive_messages()` - Long polling (20s)
- `delete_message()` - Acknowledge processed
- `get_queue_attributes()` - Queue depth monitoring

## Authentication & Identity

**Auth Provider:**
- None (internal tool, no user authentication)
- AWS credentials via environment/IAM roles

**API Authentication:**
- Serper: API key in header
- Groq: Bearer token
- AWS: Environment credentials or IAM role
- Nominatim: User-Agent header only

## Monitoring & Observability

**Error Tracking:**
- None (errors logged to stdout via loguru)

**Logs:**
- `loguru` for structured logging
- Timestamped output to stdout
- Log level controlled per-module

**Metrics:**
- Detection errors stored in `detection_errors` table
- Scrape stats tracked in `ScrapeStats` model
- No external metrics service

## CI/CD & Deployment

**Hosting:**
- Local development on macOS
- EC2 workers for detection/enrichment
- Supabase for production PostgreSQL

**CI Pipeline:**
- None detected (manual deployment)

**Deployment Pattern:**
- Local workflows: `uv run python -m workflows.<name>`
- EC2 workers: systemd services + cron jobs
- See `workflows.yaml` for full pipeline definition

## Environment Configuration

**Required Environment Variables:**
```bash
# Database (required)
SADIE_DB_HOST
SADIE_DB_PORT
SADIE_DB_NAME
SADIE_DB_USER
SADIE_DB_PASSWORD

# AWS (required for queue/storage)
AWS_REGION
SQS_DETECTION_QUEUE_URL
S3_BUCKET_NAME

# APIs (required for specific features)
SERPER_API_KEY                      # Hotel scraping
ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY  # Room count enrichment

# Optional
SLACK_WEBHOOK_URL                   # Notifications
SLACK_LEADS_WEBHOOK_URL             # Lead alerts
```

**Secrets Location:**
- `.env` file in project root (gitignored)
- `.env.example` provides template
- EC2: Environment variables or AWS Secrets Manager

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- Slack notifications on export completion
- Slack error notifications on workflow failures

## Database Schema

**Core Tables:**
- `hotels` - Scraped hotel data
- `booking_engines` - Known booking engine definitions
- `hotel_booking_engines` - Hotel-to-engine mappings
- `hotel_room_count` - Enrichment data
- `hotel_customer_proximity` - Spatial proximity data
- `existing_customers` - Reference customer locations
- `detection_errors` - Error logging
- `scrape_regions` - Polygon scrape targets
- `scrape_target_cities` - City scrape targets

**Queries:**
- Raw SQL files in `db/queries/*.sql`
- Loaded via aiosql at startup
- No ORM - direct SQL with asyncpg

## Rate Limits & Quotas

**Serper (Google Maps):**
- Free tier: Limited credits
- Paid: $50/month = 50K queries
- Rate: Managed via semaphore (4 concurrent)

**Groq LLM:**
- Free tier: 30 RPM (requests per minute)
- Paid: 1000 RPM
- Handled via `free_tier` parameter in enrichment

**Nominatim:**
- 1 request per second (strict)
- Rate limiting via `asyncio.sleep(1.1)`

**AWS SQS:**
- No practical limits for this use case
- Batch operations limited to 10 messages

---

*Integration audit: 2026-01-20*
