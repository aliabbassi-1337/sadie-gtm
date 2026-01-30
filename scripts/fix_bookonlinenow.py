"""Fix misclassified BookOnlineNow hotels.

These were incorrectly classified as SiteMinder but are actually BookOnlineNow.
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def main():
    conn = await asyncpg.connect(
        host=os.getenv("SADIE_DB_HOST"),
        port=int(os.getenv("SADIE_DB_PORT", 5432)),
        user=os.getenv("SADIE_DB_USER"),
        password=os.getenv("SADIE_DB_PASSWORD"),
        database=os.getenv("SADIE_DB_NAME"),
        ssl="require",
        statement_cache_size=0,
    )
    
    # 1. Create BookOnlineNow engine if not exists
    row = await conn.fetchrow(
        "SELECT id FROM sadie_gtm.booking_engines WHERE name = 'BookOnlineNow'"
    )
    if row:
        bon_id = row[0]
        print(f"BookOnlineNow engine already exists: id={bon_id}")
    else:
        bon_id = await conn.fetchval("""
            INSERT INTO sadie_gtm.booking_engines (name, domains, tier)
            VALUES ('BookOnlineNow', 'book-onlinenow.net,bookonlinenow.net', 2)
            RETURNING id
        """)
        print(f"Created BookOnlineNow engine: id={bon_id}")
    
    # 2. Move misclassified hotels from SiteMinder (14) to BookOnlineNow
    result = await conn.execute("""
        UPDATE sadie_gtm.hotel_booking_engines
        SET booking_engine_id = $1, updated_at = NOW()
        WHERE booking_engine_id = 14
        AND (booking_url LIKE '%book-onlinenow%' OR booking_url LIKE '%bookonlinenow%')
    """, bon_id)
    print(f"Moved to BookOnlineNow: {result}")
    
    # 3. Verify counts
    row = await conn.fetchrow(
        "SELECT COUNT(*) FROM sadie_gtm.hotel_booking_engines WHERE booking_engine_id = 14"
    )
    print(f"SiteMinder remaining: {row[0]}")
    
    row = await conn.fetchrow(
        "SELECT COUNT(*) FROM sadie_gtm.hotel_booking_engines WHERE booking_engine_id = $1",
        bon_id
    )
    print(f"BookOnlineNow total: {row[0]}")
    
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
