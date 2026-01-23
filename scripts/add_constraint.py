import asyncio
import os
from dotenv import load_dotenv
import asyncpg

load_dotenv()

async def run():
    conn = await asyncpg.connect(
        host=os.getenv('SADIE_DB_HOST'),
        port=int(os.getenv('SADIE_DB_PORT', 5432)),
        database=os.getenv('SADIE_DB_NAME'),
        user=os.getenv('SADIE_DB_USER'),
        password=os.getenv('SADIE_DB_PASSWORD'),
    )

    # Debug: check table state
    total = await conn.fetchval('SELECT COUNT(*) FROM sadie_gtm.hotels')
    print(f'Total hotels: {total}')

    constraint = await conn.fetch('''
        SELECT constraint_name FROM information_schema.table_constraints
        WHERE table_schema = 'sadie_gtm' AND table_name = 'hotels' AND constraint_type = 'UNIQUE'
    ''')
    print(f'Unique constraints: {[r["constraint_name"] for r in constraint]}')

    dupes = await conn.fetchval('SELECT COUNT(*) FROM (SELECT source FROM sadie_gtm.hotels GROUP BY source HAVING COUNT(*) > 1) t')
    print(f'Duplicate sources remaining: {dupes}')

    await conn.close()

asyncio.run(run())
