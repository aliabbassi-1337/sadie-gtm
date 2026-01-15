# Task: Scrape Consumer via SQS

## Goal
Allow scraping to run on EC2 instead of locally, triggered by SQS messages. This removes the need to keep your laptop open during scraping.

## Current Flow
```
Local: scrape → enqueue_detection → SQS
EC2: detection → enrichment → launch
```

## Target Flow
```
Local: send scrape message → SQS (scrape queue)
EC2: scrape_consumer → scrape → enqueue_detection → detection → enrichment → launch
```

## Implementation

### 1. Create `messages/` folder structure
Define SQS message types and their handlers in a central place:

```
messages/
├── __init__.py
├── base.py           # Base message class, registry
├── scrape.py         # ScrapeRegion message + handler
├── export.py         # ExportRegion message + handler (future)
└── handlers.py       # Message dispatcher
```

Example message definition:
```python
# messages/scrape.py
from dataclasses import dataclass
from messages.base import Message, handler

@dataclass
class ScrapeRegion(Message):
    queue = "scrape-queue"

    city: str | None
    state: str
    country: str

@handler(ScrapeRegion)
async def handle_scrape_region(msg: ScrapeRegion):
    """Scrape the region and enqueue results for detection."""
    from workflows.scrape_region import scrape_region
    from workflows.enqueue_detection import enqueue_hotels

    # Scrape
    hotel_ids = await scrape_region(msg.city, msg.state, msg.country)

    # Auto-enqueue to detection
    await enqueue_hotels(hotel_ids)
```

### 2. Create `scrape_consumer.py` workflow
Polls the scrape queue and dispatches to handlers:

```python
# workflows/scrape_consumer.py
from messages.handlers import consume_queue

async def main():
    await consume_queue("scrape-queue")
```

### 3. Add scrape queue to AWS
```bash
aws sqs create-queue --queue-name scrape-queue --region eu-north-1
```

### 4. Update workflows.yaml
```yaml
ec2:
  scrape_consumer:
    description: Poll scrape queue and process regions
    command: uv run python workflows/scrape_consumer.py
    type: systemd
```

## Usage
```bash
# Send scrape job from anywhere
aws sqs send-message \
  --queue-url $SCRAPE_QUEUE_URL \
  --message-body '{"type": "ScrapeRegion", "state": "Florida", "country": "USA"}'
```

## Benefits
- Laptop doesn't need to stay open
- Can queue multiple regions
- Single consumer = rate limits controlled
- Extensible: add more message types (Export, Reprocess, etc.)

## Future Messages
- `ExportRegion` - trigger exports via Slack
- `ReprocessFailed` - retry failed detections
- `NotifyComplete` - send Slack notification when region is done
