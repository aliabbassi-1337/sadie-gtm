"""Master pipeline for crawl data enrichment.

Orchestrates the full enrichment pipeline:
1. Enqueue Cloudbeds hotels to SQS (EC2 consumers process automatically)
2. Wait for SQS queue to drain
3. Run Serper geocoding for remaining hotels (SiteMinder, Mews, etc.)
4. Export to Excel
5. Sync to SharePoint

Prerequisites:
- EC2 consumers must be running as systemd services
- SERPER_API_KEY must be set

Usage:
    # Full pipeline
    uv run python -m workflows.enrich_crawl_pipeline
    
    # Skip Cloudbeds (already done)
    uv run python -m workflows.enrich_crawl_pipeline --skip-cloudbeds
    
    # Dry run (show what would happen)
    uv run python -m workflows.enrich_crawl_pipeline --dry-run
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import subprocess
import time
from loguru import logger

from db.client import init_db, close_db, get_conn
from services.enrichment import repo
from infra.sqs import get_queue_attributes, send_messages_batch

CLOUDBEDS_QUEUE_URL = os.getenv("SQS_CLOUDBEDS_ENRICHMENT_QUEUE_URL", "")


async def get_pipeline_status():
    """Get current status of crawl data."""
    async with get_conn() as conn:
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE source LIKE '%crawl%'"
        )
        missing_name = await conn.fetchval("""
            SELECT COUNT(*) FROM sadie_gtm.hotels 
            WHERE source LIKE '%crawl%' 
            AND (name IS NULL OR name = '' OR name LIKE 'Unknown%')
        """)
        missing_city = await conn.fetchval("""
            SELECT COUNT(*) FROM sadie_gtm.hotels 
            WHERE source LIKE '%crawl%' 
            AND (city IS NULL OR city = '')
        """)
        export_ready = await conn.fetchval("""
            SELECT COUNT(*) FROM sadie_gtm.hotels 
            WHERE source LIKE '%crawl%' 
            AND name IS NOT NULL AND name != '' AND name NOT LIKE 'Unknown%'
            AND city IS NOT NULL AND city != ''
        """)
        
        return {
            "total": total,
            "missing_name": missing_name,
            "missing_city": missing_city,
            "export_ready": export_ready,
        }


async def step_1_enqueue_cloudbeds(dry_run: bool = False):
    """Step 1: Enqueue Cloudbeds hotels for distributed enrichment."""
    logger.info("=" * 60)
    logger.info("STEP 1: Enqueue Cloudbeds hotels")
    logger.info("=" * 60)
    
    # Get count
    count = await repo.get_cloudbeds_hotels_needing_enrichment_count()
    logger.info(f"Cloudbeds hotels needing enrichment: {count}")
    
    if count == 0:
        logger.info("No Cloudbeds hotels need enrichment, skipping")
        return 0
    
    if dry_run:
        logger.info(f"[DRY RUN] Would enqueue {count} hotels")
        return count
    
    # Enqueue
    candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=count)
    messages = [{"hotel_id": h.id, "booking_url": h.booking_url} for h in candidates]
    
    sent = 0
    for i in range(0, len(messages), 10):
        batch = messages[i:i+10]
        if send_messages_batch(CLOUDBEDS_QUEUE_URL, batch):
            sent += len(batch)
    
    logger.info(f"Enqueued {sent}/{count} hotels")
    return sent


async def step_2_wait_for_cloudbeds(dry_run: bool = False, timeout_minutes: int = 60):
    """Step 2: Wait for SQS queue to drain (EC2 consumers processing)."""
    logger.info("=" * 60)
    logger.info("STEP 2: Wait for Cloudbeds enrichment to complete")
    logger.info("=" * 60)
    
    if dry_run:
        logger.info("[DRY RUN] Would wait for queue to drain")
        return True
    
    start = time.time()
    timeout = timeout_minutes * 60
    
    while True:
        attrs = get_queue_attributes(CLOUDBEDS_QUEUE_URL)
        waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        
        if waiting == 0 and in_flight == 0:
            logger.info("Queue is empty, Cloudbeds enrichment complete!")
            return True
        
        elapsed = time.time() - start
        if elapsed > timeout:
            logger.warning(f"Timeout after {timeout_minutes} minutes. {waiting} still waiting.")
            return False
        
        logger.info(f"  Queue: {waiting} waiting, {in_flight} in-flight. Elapsed: {int(elapsed)}s")
        await asyncio.sleep(30)  # Check every 30 seconds


async def step_3_serper_geocoding(dry_run: bool = False):
    """Step 3: Run Serper geocoding for remaining hotels."""
    logger.info("=" * 60)
    logger.info("STEP 3: Serper geocoding for remaining hotels")
    logger.info("=" * 60)
    
    # Check how many need geocoding
    async with get_conn() as conn:
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM sadie_gtm.hotels h
            WHERE h.source LIKE '%crawl%'
            AND h.name IS NOT NULL AND h.name != '' AND h.name NOT LIKE 'Unknown%'
            AND (h.city IS NULL OR h.city = '')
        """)
    
    logger.info(f"Hotels with name but no city: {count}")
    
    if count == 0:
        logger.info("No hotels need geocoding, skipping")
        return 0
    
    if dry_run:
        logger.info(f"[DRY RUN] Would geocode {count} hotels (~${count * 0.001:.2f})")
        return count
    
    # Run geocoding workflow
    cmd = [
        "uv", "run", "python", "-m", "workflows.geocode_by_name",
        "--source", "crawl",
        "--limit", str(count),
        "--no-notify"
    ]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    
    return count if result.returncode == 0 else 0


async def step_4_export(dry_run: bool = False):
    """Step 4: Export to Excel."""
    logger.info("=" * 60)
    logger.info("STEP 4: Export to Excel")
    logger.info("=" * 60)
    
    if dry_run:
        logger.info("[DRY RUN] Would export crawl data to Excel")
        return True
    
    # Run export
    cmd = ["uv", "run", "python", "-m", "workflows.export", "--source", "crawl"]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    
    return result.returncode == 0


def step_5_sync_sharepoint(dry_run: bool = False):
    """Step 5: Sync to SharePoint."""
    logger.info("=" * 60)
    logger.info("STEP 5: Sync to SharePoint")
    logger.info("=" * 60)
    
    if dry_run:
        logger.info("[DRY RUN] Would sync reports to SharePoint")
        return True
    
    # Run sync script
    script_path = Path(__file__).parent.parent / "scripts" / "sync_reports.sh"
    if not script_path.exists():
        logger.warning(f"Sync script not found: {script_path}")
        return False
    
    cmd = ["bash", str(script_path)]
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    
    return result.returncode == 0


async def run_pipeline(
    skip_cloudbeds: bool = False,
    skip_serper: bool = False,
    skip_export: bool = False,
    skip_sync: bool = False,
    dry_run: bool = False,
):
    """Run the full enrichment pipeline."""
    await init_db()
    
    try:
        # Show initial status
        logger.info("=" * 60)
        logger.info("CRAWL DATA ENRICHMENT PIPELINE")
        logger.info("=" * 60)
        
        status = await get_pipeline_status()
        logger.info(f"Total crawl hotels: {status['total']:,}")
        logger.info(f"Missing name: {status['missing_name']:,}")
        logger.info(f"Missing city: {status['missing_city']:,}")
        logger.info(f"Export-ready: {status['export_ready']:,}")
        logger.info("")
        
        if dry_run:
            logger.info(">>> DRY RUN MODE - No changes will be made <<<")
            logger.info("")
        
        # Step 1: Enqueue Cloudbeds
        if not skip_cloudbeds:
            await step_1_enqueue_cloudbeds(dry_run=dry_run)
            logger.info("")
            
            # Step 2: Wait for queue
            if not dry_run:
                success = await step_2_wait_for_cloudbeds(dry_run=dry_run)
                if not success:
                    logger.error("Cloudbeds enrichment timed out")
                logger.info("")
        else:
            logger.info("Skipping Cloudbeds enrichment (--skip-cloudbeds)")
            logger.info("")
        
        # Step 3: Serper geocoding
        if not skip_serper:
            await step_3_serper_geocoding(dry_run=dry_run)
            logger.info("")
        else:
            logger.info("Skipping Serper geocoding (--skip-serper)")
            logger.info("")
        
        # Step 4: Export
        if not skip_export:
            await step_4_export(dry_run=dry_run)
            logger.info("")
        else:
            logger.info("Skipping export (--skip-export)")
            logger.info("")
        
        # Step 5: Sync
        if not skip_sync:
            step_5_sync_sharepoint(dry_run=dry_run)
            logger.info("")
        else:
            logger.info("Skipping SharePoint sync (--skip-sync)")
            logger.info("")
        
        # Final status
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        
        final_status = await get_pipeline_status()
        logger.info(f"Export-ready: {final_status['export_ready']:,} / {final_status['total']:,}")
        
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(description="Crawl data enrichment pipeline")
    parser.add_argument("--skip-cloudbeds", action="store_true", help="Skip Cloudbeds enrichment")
    parser.add_argument("--skip-serper", action="store_true", help="Skip Serper geocoding")
    parser.add_argument("--skip-export", action="store_true", help="Skip Excel export")
    parser.add_argument("--skip-sync", action="store_true", help="Skip SharePoint sync")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    
    args = parser.parse_args()
    
    asyncio.run(run_pipeline(
        skip_cloudbeds=args.skip_cloudbeds,
        skip_serper=args.skip_serper,
        skip_export=args.skip_export,
        skip_sync=args.skip_sync,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
