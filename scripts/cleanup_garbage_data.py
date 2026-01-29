#!/usr/bin/env python3
"""
Clean up garbage hotel data:
- Delete Cloudbeds hotels with dead URLs
- Delete SiteMinder hotels with corrupted data
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger
from db.client import get_conn


async def get_counts():
    """Get counts of records to delete."""
    async with get_conn() as conn:
        # Cloudbeds dead URLs
        r = await conn.fetchrow("""
            SELECT COUNT(*) FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
            WHERE bes.name = 'Cloudbeds' AND h.name = 'Unknown' 
            AND hbe.enrichment_status = 'dead'
        """)
        cloudbeds_dead = r[0]
        
        # SiteMinder corrupted
        r = await conn.fetchrow("""
            SELECT COUNT(*) FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
            WHERE bes.name = 'SiteMinder' 
            AND (h.name = 'Hotel Website Builder' OR h.name = 'Book Online Now')
        """)
        siteminder_corrupted = r[0]
        
        # Cloudbeds unattempted
        r = await conn.fetchrow("""
            SELECT COUNT(*) FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
            WHERE bes.name = 'Cloudbeds' AND h.name = 'Unknown' 
            AND (hbe.enrichment_status IS NULL OR hbe.enrichment_status = '')
        """)
        cloudbeds_unattempted = r[0]
        
        return {
            "cloudbeds_dead": cloudbeds_dead,
            "siteminder_corrupted": siteminder_corrupted,
            "cloudbeds_unattempted": cloudbeds_unattempted,
        }


async def delete_cloudbeds_dead(dry_run: bool = True):
    """Delete Cloudbeds hotels with dead URLs."""
    async with get_conn() as conn:
        # Get hotel IDs first
        rows = await conn.fetch("""
            SELECT h.id FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
            WHERE bes.name = 'Cloudbeds' AND h.name = 'Unknown' 
            AND hbe.enrichment_status = 'dead'
        """)
        hotel_ids = [r[0] for r in rows]
        
        if not hotel_ids:
            logger.info("No Cloudbeds dead URLs to delete")
            return 0
        
        if dry_run:
            logger.info(f"[DRY RUN] Would delete {len(hotel_ids)} Cloudbeds dead URL hotels")
            return len(hotel_ids)
        
        # Delete HBE first (FK constraint)
        await conn.execute("""
            DELETE FROM sadie_gtm.hotel_booking_engines
            WHERE hotel_id = ANY($1)
        """, hotel_ids)
        
        # Delete hotels
        result = await conn.execute("""
            DELETE FROM sadie_gtm.hotels
            WHERE id = ANY($1)
        """, hotel_ids)
        
        logger.success(f"Deleted {len(hotel_ids)} Cloudbeds dead URL hotels")
        return len(hotel_ids)


async def delete_siteminder_corrupted(dry_run: bool = True):
    """Delete SiteMinder hotels with corrupted data."""
    async with get_conn() as conn:
        # Get hotel IDs first
        rows = await conn.fetch("""
            SELECT h.id FROM sadie_gtm.hotels h
            JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
            JOIN sadie_gtm.booking_engines bes ON bes.id = hbe.booking_engine_id
            WHERE bes.name = 'SiteMinder' 
            AND (h.name = 'Hotel Website Builder' OR h.name = 'Book Online Now')
        """)
        hotel_ids = [r[0] for r in rows]
        
        if not hotel_ids:
            logger.info("No SiteMinder corrupted data to delete")
            return 0
        
        if dry_run:
            logger.info(f"[DRY RUN] Would delete {len(hotel_ids)} SiteMinder corrupted hotels")
            return len(hotel_ids)
        
        # Delete HBE first (FK constraint)
        await conn.execute("""
            DELETE FROM sadie_gtm.hotel_booking_engines
            WHERE hotel_id = ANY($1)
        """, hotel_ids)
        
        # Delete hotels
        result = await conn.execute("""
            DELETE FROM sadie_gtm.hotels
            WHERE id = ANY($1)
        """, hotel_ids)
        
        logger.success(f"Deleted {len(hotel_ids)} SiteMinder corrupted hotels")
        return len(hotel_ids)


async def main(dry_run: bool = True, task: str = "all"):
    """Main entry point."""
    logger.info("Checking counts...")
    counts = await get_counts()
    
    logger.info(f"Cloudbeds dead URLs: {counts['cloudbeds_dead']}")
    logger.info(f"SiteMinder corrupted: {counts['siteminder_corrupted']}")
    logger.info(f"Cloudbeds unattempted (won't delete): {counts['cloudbeds_unattempted']}")
    
    if task in ("all", "cloudbeds"):
        await delete_cloudbeds_dead(dry_run)
    
    if task in ("all", "siteminder"):
        await delete_siteminder_corrupted(dry_run)
    
    if dry_run:
        logger.info("This was a dry run. Use --execute to delete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up garbage hotel data")
    parser.add_argument("--execute", action="store_true", help="Actually delete (default is dry run)")
    parser.add_argument("--task", choices=["all", "cloudbeds", "siteminder"], default="all")
    args = parser.parse_args()
    
    asyncio.run(main(dry_run=not args.execute, task=args.task))
