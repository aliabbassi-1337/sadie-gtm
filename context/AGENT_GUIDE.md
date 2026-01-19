# Agent Guide: Sadie GTM

This guide teaches agents how to work with the Sadie GTM hotel lead generation pipeline.

## Project Overview

Sadie GTM scrapes hotels from Google, detects their booking engines, enriches them with room counts and customer proximity data, then exports leads to Excel reports.

**Pipeline:** Ingest → Scrape → Detect → Enrich → Launch → Export

## Directory Structure

```
sadie_gtm/
├── workflows/           # CLI entry points for pipeline stages
├── services/            # Business logic (enrichment, leadgen, detection)
├── db/
│   ├── client.py        # Database connection (asyncpg)
│   ├── queries/         # SQL files (aiosql format)
│   └── models/          # Pydantic models
├── scripts/             # Utility scripts (deploy, sync)
├── infra/               # EC2/cron configuration
└── context/             # Documentation for agents
```

## Common Commands

### Check Pipeline Status

```bash
# Enrichment status (room counts + proximity)
uv run python workflows/enrichment.py status

# Launcher status (hotels ready to launch)
uv run python workflows/launcher.py status

# Database counts
uv run python -c "
import asyncio
from db.client import init_db, close_db, get_conn

async def check():
    await init_db()
    async with get_conn() as conn:
        r = await conn.fetchrow('SELECT COUNT(*) FROM sadie_gtm.hotels WHERE status = 1')
        print(f'Launched hotels: {r[0]}')
    await close_db()

asyncio.run(check())
"
```

### Run Enrichment

```bash
# Room count enrichment (uses Groq LLM)
uv run python workflows/enrichment.py room-counts --limit 100

# Customer proximity calculation (PostGIS)
uv run python workflows/enrichment.py proximity --limit 100

# Location enrichment (reverse geocoding for missing cities)
uv run python workflows/location_enrichment.py enrich --limit 100
```

### Export Reports

```bash
# Export all FL cities to S3
uv run python workflows/export.py --state FL

# Sync reports to OneDrive
./scripts/sync_reports.sh
```

### Database Queries

The project uses aiosql for SQL queries. Query files are in `db/queries/`.

```python
# Example: Run a query
from db.client import init_db, close_db, queries, get_conn

async with get_conn() as conn:
    # Named queries from SQL files
    hotels = await queries.get_hotels_pending_detection(conn, limit=100)

    # Raw queries
    result = await conn.fetch("SELECT * FROM sadie_gtm.hotels LIMIT 5")
```

### Retry Failed Enrichments

```python
# Delete failed records to allow retry
async with get_conn() as conn:
    await conn.execute('''
        DELETE FROM sadie_gtm.hotel_room_count WHERE status = 0
    ''')

# Then run enrichment again
uv run python workflows/enrichment.py room-counts --limit 100
```

## EC2 Management

### SSH to EC2

```bash
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117
```

### Project Location on EC2

```bash
cd ~/sadie-gtm
```

### Update Code on EC2

```bash
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "cd ~/sadie-gtm && git pull"
```

### Check/Update Cron Jobs

```bash
# View current cron
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "cat /etc/cron.d/sadie-gtm"

# Regenerate cron from workflows.yaml
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "cd ~/sadie-gtm && /home/ubuntu/.local/bin/uv run python scripts/deploy_ec2.py generate"

# Deploy new cron
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "sudo cp ~/sadie-gtm/infra/ec2/generated/sadie-cron /etc/cron.d/sadie-gtm"
```

### Check EC2 Logs

```bash
# Enrichment logs
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "tail -50 /var/log/sadie/enrichment-room-counts.log"

# Proximity logs
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "tail -50 /var/log/sadie/enrichment-proximity.log"

# Launcher logs
ssh -i ~/.ssh/m3-air.pem ubuntu@13.51.169.117 "tail -50 /var/log/sadie/launcher.log"
```

## Workflows Configuration

Cron jobs are defined in `workflows.yaml`. After editing:

1. Commit and push changes
2. Pull on EC2: `git pull`
3. Regenerate: `uv run python scripts/deploy_ec2.py generate`
4. Deploy: `sudo cp infra/ec2/generated/sadie-cron /etc/cron.d/sadie-gtm`

## Database Schema

Key tables in `sadie_gtm` schema:

| Table | Purpose |
|-------|---------|
| `hotels` | Main hotel data (name, website, location, status) |
| `hotel_booking_engines` | Detected booking engine per hotel |
| `booking_engines` | Booking engine definitions (tier 1/2/3) |
| `hotel_room_count` | Enriched room counts |
| `hotel_customer_proximity` | Distance to nearest existing customer |
| `existing_customers` | Sadie's current customers |
| `scrape_target_cities` | Cities/regions to scrape |

### Hotel Status Values

```
-3 = duplicate
-2 = location_mismatch (rejected)
-1 = no_booking_engine (rejected)
 0 = pending (in pipeline)
 1 = launched (live lead)
```

## Git Workflow

### Create Feature Branch

```bash
git checkout -b feat/my-feature
# ... make changes ...
git add .
git commit -m "feat: description"
git push -u origin feat/my-feature
```

### Create PR

```bash
gh pr create --title "feat: description" --body "## Summary
- Change 1
- Change 2

## Test plan
- [ ] Test 1
"
```

### Create Worktree (parallel development)

```bash
git worktree add ../sadie-gtm-feature -b feat/feature-name
cd ../sadie-gtm-feature
```

## Troubleshooting

### Enrichment Not Running on EC2

1. Check if cron exists: `cat /etc/cron.d/sadie-gtm`
2. Check logs for errors: `tail -50 /var/log/sadie/enrichment-room-counts.log`
3. Verify code is updated: `cd ~/sadie-gtm && git log -1`
4. Test manually: `source .env && uv run python workflows/enrichment.py status`

### Websites Failing to Fetch

Some hotels fail enrichment because their websites are:
- Down or unreachable
- Blocking automated requests
- Have SSL certificate issues

These are marked as `status=0` in `hotel_room_count`. To retry:
```sql
DELETE FROM sadie_gtm.hotel_room_count WHERE status = 0;
```

### Missing City Data

Hotels with coordinates but no city can be enriched:
```bash
uv run python workflows/location_enrichment.py enrich --limit 100
```

Uses Nominatim reverse geocoding (1 req/sec rate limit).
