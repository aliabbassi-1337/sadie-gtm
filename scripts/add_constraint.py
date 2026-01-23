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
    await conn.execute('ALTER TABLE sadie_gtm.hotels ADD CONSTRAINT hotels_source_unique UNIQUE (source);')
    await conn.close()
    print('Done')

asyncio.run(run())
