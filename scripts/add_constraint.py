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

    print('Dropping old name+city+website constraint...')
    await conn.execute('DROP INDEX IF EXISTS sadie_gtm.idx_hotels_name_city_website_unique')
    print('Done')

    await conn.close()

asyncio.run(run())
