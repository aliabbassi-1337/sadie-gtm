import asyncio
from db.client import init_db
from services.leadgen.service import Service

async def scraper_workflow():
    service = Service()
    scraped = await service.scrape_city(["Miami"])
    if scraped:
        print("yeet")

async def run():
    await init_db()
    await scraper_workflow()

asyncio.run(run())
