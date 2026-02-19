"""Audit Big4 decision makers data."""
import asyncio
import asyncpg

DB_CONFIG = dict(
    host='aws-1-ap-southeast-1.pooler.supabase.com',
    port=6543, database='postgres',
    user='postgres.yunairadgmaqesxejqap',
    password='SadieGTM321-',
    statement_cache_size=0,
)

async def main():
    conn = await asyncpg.connect(**DB_CONFIG)

    # User's query (adapted - Big4 may not have source='big4', use name filter)
    rows = await conn.fetch("""
        SELECT
            h.id AS hotel_id,
            h.name AS hotel_name,
            h.website,
            COUNT(dm.*) AS contact_count,
            string_agg(dm.full_name, ', ' ORDER BY dm.confidence DESC) AS contacts,
            string_agg(dm.title, ', ' ORDER BY dm.confidence DESC) AS titles,
            string_agg(dm.email, ', ' ORDER BY dm.confidence DESC) AS emails,
            string_agg(CASE WHEN dm.email_verified THEN 'Y' ELSE 'N' END, ', ' ORDER BY dm.confidence DESC) AS verified,
            string_agg(dm.phone, ', ' ORDER BY dm.confidence DESC) AS phones,
            MAX(dm.confidence) AS best_confidence,
            string_agg(array_to_string(dm.sources, '+'), ', ' ORDER BY dm.confidence DESC) AS sources
        FROM sadie_gtm.hotels h
        JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id
        WHERE (h.external_id_type = 'big4' OR h.source LIKE '%::big4%')
        GROUP BY h.id, h.name, h.website
        ORDER BY contact_count DESC
    """)

    print(f"Hotels with decision makers: {len(rows)}\n")
    for r in rows:
        print(f"{'='*70}")
        print(f"Hotel: {r['hotel_name']} (id={r['hotel_id']})")
        print(f"Website: {r['website']}")
        print(f"Contacts ({r['contact_count']}): {r['contacts']}")
        print(f"Titles: {r['titles']}")
        print(f"Emails: {r['emails']}")
        print(f"Verified: {r['verified']}")
        print(f"Phones: {r['phones']}")
        print(f"Confidence: {r['best_confidence']}")
        print(f"Sources: {r['sources']}")

    # Bug checks
    print(f"\n{'='*70}")
    print("BUG AUDIT")
    print(f"{'='*70}")

    # 1. First-name-only contacts (no surname)
    first_only = await conn.fetch("""
        SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name, dm.hotel_id
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE (h.external_id_type = 'big4' OR h.source LIKE '%::big4%')
          AND dm.full_name NOT LIKE '% %'
    """)
    print(f"\n1. FIRST NAME ONLY (no surname): {len(first_only)}")
    for r in first_only:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    # 2. Duplicate contacts (same hotel, same name)
    dupes = await conn.fetch("""
        SELECT dm.hotel_id, dm.full_name, COUNT(*) as cnt, h.name AS hotel_name
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE h.name ILIKE '%big4%' OR h.name ILIKE '%big 4%'
        GROUP BY dm.hotel_id, dm.full_name, h.name
        HAVING COUNT(*) > 1
    """)
    print(f"\n2. DUPLICATE NAMES (same hotel): {len(dupes)}")
    for r in dupes:
        print(f"   '{r['full_name']}' x{r['cnt']} -> {r['hotel_name']} (hotel_id={r['hotel_id']})")

    # 3. Same person across duplicate hotel entries
    cross_dupes = await conn.fetch("""
        SELECT dm.full_name, COUNT(DISTINCT dm.hotel_id) as hotel_count,
               string_agg(DISTINCT h.name, ' | ') as hotel_names
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE h.name ILIKE '%big4%' OR h.name ILIKE '%big 4%'
        GROUP BY dm.full_name
        HAVING COUNT(DISTINCT dm.hotel_id) > 1
    """)
    print(f"\n3. SAME NAME ACROSS MULTIPLE HOTEL IDs: {len(cross_dupes)}")
    for r in cross_dupes:
        print(f"   '{r['full_name']}' in {r['hotel_count']} hotels: {r['hotel_names']}")

    # 4. Suspicious names (too short, nicknames, not real names)
    suspicious = await conn.fetch("""
        SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE (h.external_id_type = 'big4' OR h.source LIKE '%::big4%')
          AND (LENGTH(dm.full_name) < 4 OR dm.full_name ~ '^[A-Z][a-z]{1,2}$')
    """)
    print(f"\n4. SUSPICIOUS SHORT NAMES: {len(suspicious)}")
    for r in suspicious:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    # 5. Non-owner titles (corporate roles that may not be relevant to the specific park)
    corp = await conn.fetch("""
        SELECT dm.id, dm.full_name, dm.title, h.name AS hotel_name
        FROM sadie_gtm.hotel_decision_makers dm
        JOIN sadie_gtm.hotels h ON h.id = dm.hotel_id
        WHERE (h.external_id_type = 'big4' OR h.source LIKE '%::big4%')
          AND dm.title ILIKE ANY(ARRAY['%CEO%', '%CFO%', '%Chief%', '%Head of%', '%New Zealand%', '%Marketing%'])
    """)
    print(f"\n5. CORPORATE/HQ ROLES (may not be park-specific): {len(corp)}")
    for r in corp:
        print(f"   dm.id={r['id']} | '{r['full_name']}' ({r['title']}) -> {r['hotel_name']}")

    # 6. Hotels with multiple IDs (duplicate hotels)
    dup_hotels = await conn.fetch("""
        SELECT LOWER(TRIM(h.name)) as norm_name, COUNT(*) as cnt,
               string_agg(h.id::text, ', ') as ids,
               COUNT(DISTINCT dm.id) as total_dms
        FROM sadie_gtm.hotels h
        LEFT JOIN sadie_gtm.hotel_decision_makers dm ON dm.hotel_id = h.id
        WHERE h.name ILIKE '%big4%' OR h.name ILIKE '%big 4%'
        GROUP BY LOWER(TRIM(h.name))
        HAVING COUNT(*) > 1 AND COUNT(DISTINCT dm.id) > 0
        ORDER BY cnt DESC
    """)
    print(f"\n6. DUPLICATE HOTEL ENTRIES with DMs: {len(dup_hotels)}")
    for r in dup_hotels:
        print(f"   '{r['norm_name']}' x{r['cnt']} (ids: {r['ids']}) -> {r['total_dms']} DMs")

    await conn.close()

asyncio.run(main())
