"""Export IPMS247 leads to Excel file.

USAGE:
    uv run python workflows/export_ipms247.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
from loguru import logger

from db.client import init_db, close_db
from services.reporting.service import Service


async def main():
    await init_db()
    try:
        service = Service()
        
        logger.info("Exporting IPMS247 leads...")
        s3_uri, count = await service.export_by_source(
            source_pattern="ipms247%",
            filename="ipms247_leads.xlsx"
        )
        
        if count > 0:
            logger.success(f"Exported {count} IPMS247 leads to {s3_uri}")
        else:
            logger.warning("No IPMS247 leads found")
            
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
