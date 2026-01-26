#!/usr/bin/env python
"""Bulk insert crawled slugs as hotels with placeholder names.

Bypasses Common Crawl entirely - just creates hotels with Unknown names.
SQS enrichment workers will scrape real names from live booking pages.
"""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import asyncpg

load_dotenv()


async def bulk_insert():
    conn = await asyncpg.connect(
        host=os.environ["SADIE_DB_HOST"],
        port=os.environ["SADIE_DB_PORT"],
        database=os.environ["SADIE_DB_NAME"],
        user=os.environ["SADIE_DB_USER"],
        password=os.environ["SADIE_DB_PASSWORD"],
    )

    # Get or create booking engine IDs
    engine_names = {
        "cloudbeds": "Cloudbeds",
        "mews": "Mews",
        "rmscloud": "RMS Cloud",
        "siteminder": "SiteMinder",
    }
    engines = {}
    
    for key, name in engine_names.items():
        row = await conn.fetchrow(
            "SELECT id FROM sadie_gtm.booking_engines WHERE name = $1", name
        )
        if row:
            engines[key] = row[0]
        else:
            engine_id = await conn.fetchval(
                "INSERT INTO sadie_gtm.booking_engines (name, tier) VALUES ($1, 1) RETURNING id",
                name,
            )
            engines[key] = engine_id

    print(f"Engines: {engines}")

    # URL patterns for booking pages
    patterns = {
        "cloudbeds": "https://hotels.cloudbeds.com/reservation/{slug}",
        "mews": "https://app.mews.com/distributor/{slug}",
        "rmscloud": "https://bookings.rmscloud.com/{slug}",
        "siteminder": "https://{slug}.book-onlinenow.net/",
    }

    files = {
        "cloudbeds": "data/crawl/cloudbeds.txt",
        "mews": "data/crawl/mews.txt",
        "rmscloud": "data/crawl/rms.txt",
        "siteminder": "data/crawl/siteminder.txt",
    }

    total_inserted = 0
    total_skipped = 0

    for engine_key, file_path in files.items():
        path = Path(file_path)
        if not path.exists():
            print(f"  {engine_key}: file not found at {file_path}")
            continue

        slugs = list(set([s.strip() for s in path.read_text().strip().split("\n") if s.strip()]))
        print(f"  {engine_key}: {len(slugs)} slugs")

        engine_id = engines.get(engine_key)
        if not engine_id:
            print(f"    No engine ID for {engine_key}")
            continue

        inserted = 0
        skipped = 0

        for i, slug in enumerate(slugs):
            if i > 0 and i % 1000 == 0:
                print(f"    Progress: {i}/{len(slugs)} ({inserted} inserted, {skipped} skipped)")

            booking_url = patterns.get(engine_key, "").replace("{slug}", slug)

            # Check if booking URL already exists
            existing = await conn.fetchval(
                "SELECT hotel_id FROM sadie_gtm.hotel_booking_engines WHERE booking_url = $1",
                booking_url,
            )
            if existing:
                skipped += 1
                continue

            # Insert hotel with placeholder name
            name = f"Unknown ({slug})"
            hotel_id = await conn.fetchval(
                """INSERT INTO sadie_gtm.hotels (name, source, external_id, external_id_type) 
                   VALUES ($1, $2, $3, $4) RETURNING id""",
                name,
                f"{engine_key}_crawl",
                slug,
                f"{engine_key}_crawl",
            )

            # Link booking engine
            await conn.execute(
                """INSERT INTO sadie_gtm.hotel_booking_engines 
                   (hotel_id, booking_engine_id, booking_url, engine_property_id, detection_method, status) 
                   VALUES ($1, $2, $3, $4, $5, 1)""",
                hotel_id,
                engine_id,
                booking_url,
                slug,
                "crawl_bulk",
            )
            inserted += 1

        print(f"    {engine_key}: Inserted {inserted}, Skipped {skipped}")
        total_inserted += inserted
        total_skipped += skipped

    await conn.close()
    print(f"\nTOTAL: {total_inserted} inserted, {total_skipped} skipped")


if __name__ == "__main__":
    asyncio.run(bulk_insert())
