#!/usr/bin/env python3
"""Fix Cloudbeds hotels with HTML entities in names."""

import asyncio
import html
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


async def main():
    conn = await asyncpg.connect(
        host=os.getenv('SADIE_DB_HOST'),
        port=int(os.getenv('SADIE_DB_PORT', 5432)),
        user=os.getenv('SADIE_DB_USER'),
        password=os.getenv('SADIE_DB_PASSWORD'),
        database=os.getenv('SADIE_DB_NAME'),
        ssl='require'
    )
    
    logger.info("=== FIX CLOUDBEDS HTML ENTITIES ===")
    
    # Get hotels with HTML entities
    rows = await conn.fetch('''
        SELECT h.id, h.name FROM sadie_gtm.hotels h
        JOIN sadie_gtm.hotel_booking_engines hbe ON hbe.hotel_id = h.id
        JOIN sadie_gtm.booking_engines be ON be.id = hbe.booking_engine_id
        WHERE be.name = 'Cloudbeds'
          AND (h.name LIKE '%&amp;%' 
               OR h.name LIKE '%&#%'
               OR h.name LIKE '%&quot;%'
               OR h.name LIKE '%&lt;%'
               OR h.name LIKE '%&gt;%')
    ''')
    logger.info(f"Found {len(rows)} hotels with HTML entities")
    
    # Fix each one using Python's html.unescape
    updated = 0
    for row in rows:
        hotel_id = row['id']
        old_name = row['name']
        new_name = html.unescape(old_name)
        if new_name != old_name:
            await conn.execute(
                'UPDATE sadie_gtm.hotels SET name = $1, updated_at = NOW() WHERE id = $2',
                new_name, hotel_id
            )
            updated += 1
            if updated <= 5:
                logger.info(f"  {old_name} -> {new_name}")
    
    logger.info(f"Updated {updated} hotels")
    
    await conn.close()
    logger.info("Done!")


if __name__ == "__main__":
    asyncio.run(main())
