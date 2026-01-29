"""Cloudbeds Enrichment Service.

Orchestrates Cloudbeds hotel enrichment using Playwright scraping.
"""

import asyncio
from typing import List, Dict, Any, Optional

from loguru import logger
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import Stealth
from pydantic import BaseModel

from lib.cloudbeds import CloudbedsScraper, ExtractedCloudbedsData
from services.enrichment import repo


class EnrichmentResult(BaseModel):
    """Result of enrichment batch."""
    processed: int
    enriched: int
    failed: int


class HotelEnrichmentResult(BaseModel):
    """Result of enriching a single hotel."""
    hotel_id: int
    success: bool
    data: Optional[ExtractedCloudbedsData] = None
    error: Optional[str] = None


class CloudbedsEnrichmentService:
    """Service for enriching Cloudbeds hotels."""
    
    async def enrich_hotels(
        self,
        limit: int = 100,
        concurrency: int = 3,
    ) -> EnrichmentResult:
        """Enrich Cloudbeds hotels by scraping their booking pages.
        
        Args:
            limit: Max hotels to process
            concurrency: Number of concurrent browser contexts
            
        Returns:
            EnrichmentResult with processed/enriched/failed counts
        """
        candidates = await repo.get_cloudbeds_hotels_needing_enrichment(limit=limit)
        hotels = [{"id": c.id, "booking_url": c.booking_url} for c in candidates]
        
        if not hotels:
            logger.info("No Cloudbeds hotels need enrichment")
            return EnrichmentResult(processed=0, enriched=0, failed=0)
        
        logger.info(f"Found {len(hotels)} Cloudbeds hotels to enrich")
        
        total_enriched = 0
        total_errors = 0
        batch_size = 50
        
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create browser contexts pool
            contexts: List[BrowserContext] = []
            scrapers: List[CloudbedsScraper] = []
            
            for _ in range(concurrency):
                ctx = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
                page = await ctx.new_page()
                contexts.append(ctx)
                scrapers.append(CloudbedsScraper(page))
            
            logger.info(f"Created {concurrency} browser contexts")
            
            results_buffer: List[HotelEnrichmentResult] = []
            processed = 0
            
            # Process in batches of concurrency
            for batch_start in range(0, len(hotels), concurrency):
                batch = hotels[batch_start:batch_start + concurrency]
                
                # Run batch concurrently
                tasks = [
                    self._enrich_one(scrapers[i], hotel)
                    for i, hotel in enumerate(batch)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, result in enumerate(results):
                    processed += 1
                    hotel = batch[i]
                    
                    if isinstance(result, Exception):
                        total_errors += 1
                        logger.warning(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: error - {result}")
                        continue
                    
                    results_buffer.append(result)
                    
                    if result.success and result.data:
                        data = result.data
                        parts = []
                        if data.name:
                            parts.append(f"name={data.name[:25]}")
                        if data.city:
                            parts.append(f"loc={data.city}, {data.state}")
                        if data.phone:
                            parts.append("phone")
                        if data.email:
                            parts.append("email")
                        logger.info(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: {', '.join(parts)}")
                    elif result.error:
                        total_errors += 1
                        logger.warning(f"  [{processed}/{len(hotels)}] Hotel {hotel['id']}: {result.error}")
                
                # Batch update DB
                if len(results_buffer) >= batch_size:
                    updated = await self._batch_update(results_buffer)
                    total_enriched += updated
                    logger.info(f"  Batch update: {updated} hotels")
                    results_buffer = []
            
            # Final batch
            if results_buffer:
                updated = await self._batch_update(results_buffer)
                total_enriched += updated
                logger.info(f"  Final batch update: {updated} hotels")
            
            # Cleanup
            for ctx in contexts:
                await ctx.close()
            await browser.close()
        
        return EnrichmentResult(
            processed=len(hotels),
            enriched=total_enriched,
            failed=total_errors,
        )
    
    async def _enrich_one(
        self,
        scraper: CloudbedsScraper,
        hotel: Dict[str, Any],
    ) -> HotelEnrichmentResult:
        """Enrich a single hotel."""
        hotel_id = hotel['id']
        booking_url = hotel['booking_url']
        
        try:
            data = await scraper.extract(booking_url)
            
            if not data:
                return HotelEnrichmentResult(
                    hotel_id=hotel_id,
                    success=False,
                    error="no_data_extracted"
                )
            
            return HotelEnrichmentResult(
                hotel_id=hotel_id,
                success=True,
                data=data,
            )
            
        except Exception as e:
            return HotelEnrichmentResult(
                hotel_id=hotel_id,
                success=False,
                error=str(e)[:100]
            )
    
    async def _batch_update(self, results: List[HotelEnrichmentResult]) -> int:
        """Batch update hotels with enrichment results."""
        successful = [r for r in results if r.success and r.data and (r.data.city or r.data.name)]
        
        if not successful:
            return 0
        
        updates = [
            {
                "hotel_id": r.hotel_id,
                "name": r.data.name,
                "address": r.data.address,
                "city": r.data.city,
                "state": r.data.state,
                "country": r.data.country,
                "phone": r.data.phone,
                "email": r.data.email,
            }
            for r in successful
        ]
        
        return await repo.batch_update_cloudbeds_enrichment(updates)
