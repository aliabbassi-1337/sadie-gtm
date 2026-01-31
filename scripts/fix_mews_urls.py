"""Fix malformed Mews booking URLs.

The archive discovery saved URL paths instead of just slugs, causing
URLs like: https://app.mews.com/distributor/app.mews.com/distributor/UUID
instead of: https://app.mews.com/distributor/UUID
"""

import asyncio
import asyncpg
import os
import re
from dotenv import load_dotenv

load_dotenv()

UUID_PATTERN = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')


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
    
    # Find bad Mews URLs
    rows = await conn.fetch("""
        SELECT hotel_id, booking_url, engine_property_id 
        FROM sadie_gtm.hotel_booking_engines 
        WHERE booking_engine_id = 4 
        AND booking_url LIKE '%distributor/app.mews.com%'
    """)
    print(f"Bad Mews URLs to fix: {len(rows)}")
    
    if not rows:
        print("No bad URLs found!")
        await conn.close()
        return
    
    # Fix each one
    fixed = 0
    for row in rows:
        hotel_id = row["hotel_id"]
        old_url = row["booking_url"]
        
        # Extract UUID from the URL
        match = UUID_PATTERN.search(old_url)
        if not match:
            print(f"  Could not extract UUID from: {old_url}")
            continue
        
        uuid = match.group(0)
        new_url = f"https://app.mews.com/distributor/{uuid}"
        
        await conn.execute("""
            UPDATE sadie_gtm.hotel_booking_engines
            SET booking_url = $1, engine_property_id = $2, updated_at = NOW()
            WHERE hotel_id = $3 AND booking_engine_id = 4
        """, new_url, uuid, hotel_id)
        fixed += 1
    
    print(f"Fixed: {fixed} URLs")
    
    # Sample fixed URLs
    print("\nSample fixed URLs:")
    rows = await conn.fetch("""
        SELECT booking_url FROM sadie_gtm.hotel_booking_engines 
        WHERE booking_engine_id = 4
        ORDER BY updated_at DESC
        LIMIT 5
    """)
    for r in rows:
        print(f"  {r['booking_url']}")
    
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
