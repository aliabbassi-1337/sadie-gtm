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
    print('Deleting duplicates...')
    deleted = await conn.execute('''
        DELETE FROM sadie_gtm.hotels a USING sadie_gtm.hotels b
        WHERE a.id < b.id AND a.source = b.source
    ''')
    print(f'Deleted: {deleted}')
    print('Adding constraint...')
    await conn.execute('ALTER TABLE sadie_gtm.hotels ADD CONSTRAINT hotels_source_unique UNIQUE (source);')
    await conn.close()
    print('Done')

asyncio.run(run())
