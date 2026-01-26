"""
Crawl Ingestor - Import hotels from crawled booking engine URLs.

Ingests slugs/URLs from Common Crawl or similar sources:
- Cloudbeds, Mews, RMS, SiteMinder

Hotels are inserted with placeholder names ("Unknown (slug)") and 
enriched later via SQS workers that scrape live booking pages.
"""

from pathlib import Path
from typing import List, Optional, AsyncIterator, Tuple

from loguru import logger

from services.ingestor.base import BaseIngestor
from services.ingestor.registry import register
from services.ingestor.models.base import IngestStats
from services.ingestor.models.crawl import CrawledHotel, URL_PATTERNS
from db.client import get_conn, queries


# Engine display names
ENGINE_NAMES = {
    "cloudbeds": "Cloudbeds",
    "mews": "Mews",
    "rms": "RMS Cloud",
    "siteminder": "SiteMinder",
}


@register("crawl")
class CrawlIngestor(BaseIngestor[CrawledHotel]):
    """
    Import hotels from crawled booking engine URLs.
    
    Usage:
        # Single engine
        ingestor = CrawlIngestor(engine="cloudbeds", file_path="data/crawl/cloudbeds.txt")
        hotels, stats = await ingestor.ingest()
        
        # All engines in directory
        ingestor = CrawlIngestor.from_directory("data/crawl")
        hotels, stats = await ingestor.ingest()
    """
    
    def __init__(
        self,
        engine: str,
        file_path: Optional[str] = None,
        slugs: Optional[List[str]] = None,
    ):
        """
        Initialize crawl ingestor.
        
        Args:
            engine: Booking engine name (cloudbeds, mews, rms, siteminder)
            file_path: Path to text file with slugs (one per line)
            slugs: Direct list of slugs (alternative to file_path)
        """
        self.engine = engine.lower()
        self.file_path = file_path
        self.slugs = slugs
        self._booking_engine_id: Optional[int] = None
    
    @property
    def source_name(self) -> str:
        """Source name for tracking."""
        return f"{self.engine}_crawl"
    
    @property
    def external_id_type(self) -> str:
        """External ID type for deduplication."""
        return f"{self.engine}_crawl"
    
    @classmethod
    def from_directory(cls, dir_path: str) -> List["CrawlIngestor"]:
        """
        Create ingestors for all crawl files in a directory.
        
        Detects engine from filename:
        - cloudbeds.txt -> cloudbeds
        - mews.txt -> mews
        - rms.txt -> rms
        - siteminder.txt -> siteminder
        """
        ingestors = []
        path = Path(dir_path)
        
        if not path.exists():
            logger.error(f"Directory not found: {dir_path}")
            return ingestors
        
        for txt_file in path.glob("*.txt"):
            filename_lower = txt_file.name.lower()
            for engine in ENGINE_NAMES.keys():
                if engine in filename_lower:
                    ingestors.append(cls(engine=engine, file_path=str(txt_file)))
                    break
        
        return ingestors
    
    async def fetch(self) -> AsyncIterator[Tuple[str, bytes]]:
        """Fetch slug data from file or direct list."""
        if self.slugs:
            # Direct slugs provided
            content = "\n".join(self.slugs).encode("utf-8")
            yield "direct_slugs.txt", content
        elif self.file_path:
            # Read from file
            path = Path(self.file_path)
            if not path.exists():
                logger.error(f"File not found: {self.file_path}")
                return
            
            content = path.read_bytes()
            yield path.name, content
        else:
            logger.error("No file_path or slugs provided")
            return
    
    def parse(self, data: bytes, filename: str = "") -> List[CrawledHotel]:
        """Parse slug list into CrawledHotel records."""
        hotels = []
        
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1")
        
        # Parse slugs (one per line)
        lines = text.strip().split("\n")
        seen_slugs = set()
        
        for line in lines:
            slug = line.strip().lower()
            if not slug or slug in seen_slugs:
                continue
            
            seen_slugs.add(slug)
            
            hotel = CrawledHotel.from_slug(
                slug=slug,
                booking_engine=self.engine,
                name=None,  # Will use placeholder
                source=self.source_name,
            )
            hotels.append(hotel)
        
        return hotels
    
    def validate(self, record: CrawledHotel) -> Optional[CrawledHotel]:
        """
        Validate a crawled hotel record.
        
        Override base to allow records without real names
        (they use placeholder names like "Unknown (slug)").
        """
        if not record.slug:
            return None
        if not record.booking_url:
            return None
        return record
    
    async def _get_or_create_booking_engine_id(self) -> Optional[int]:
        """Get or create the booking engine ID using proper queries."""
        if self._booking_engine_id:
            return self._booking_engine_id
        
        engine_name = ENGINE_NAMES.get(self.engine, self.engine.title())
        
        async with get_conn() as conn:
            # Try to get existing
            row = await queries.get_booking_engine_by_name(conn, name=engine_name)
            if row:
                self._booking_engine_id = row["id"]
                return self._booking_engine_id
            
            # Create new
            engine_id = await queries.insert_booking_engine(
                conn,
                name=engine_name,
                domains=None,
                tier=1,
            )
            self._booking_engine_id = engine_id
            return engine_id
    
    async def _batch_save(self, records: List[CrawledHotel], batch_size: int = 500) -> int:
        """
        Batch insert crawled hotels with booking engine linking.
        
        For each hotel:
        1. Check if booking_url already exists -> skip
        2. Insert hotel with placeholder name
        3. Link to booking engine in hotel_booking_engines
        """
        if not records:
            return 0
        
        engine_id = await self._get_or_create_booking_engine_id()
        if not engine_id:
            logger.error(f"Failed to get booking engine ID for {self.engine}")
            return 0
        
        saved = 0
        skipped = 0
        
        async with get_conn() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                for record in batch:
                    try:
                        # Check if booking URL already exists
                        existing = await queries.get_hotel_by_booking_url(
                            conn, booking_url=record.booking_url
                        )
                        if existing:
                            skipped += 1
                            continue
                        
                        # Insert hotel with placeholder name
                        hotel_id = await queries.insert_hotel_with_external_id(
                            conn,
                            name=record.name,
                            website=None,
                            source=record.source,
                            status=0,  # PENDING
                            address=None,
                            city=None,
                            state=None,
                            country="USA",
                            phone=None,
                            category=None,
                            external_id=record.external_id,
                            external_id_type=record.external_id_type,
                        )
                        
                        if not hotel_id:
                            continue
                        
                        # Link booking engine
                        await queries.insert_hotel_booking_engine(
                            conn,
                            hotel_id=hotel_id,
                            booking_engine_id=engine_id,
                            booking_url=record.booking_url,
                            engine_property_id=record.slug,
                            detection_method=record.detection_method,
                            status=1,
                        )
                        
                        saved += 1
                        
                    except Exception as e:
                        logger.debug(f"Error saving {record.slug}: {e}")
                        continue
                
                logger.info(f"  Batch {i // batch_size + 1}: {i + len(batch)}/{len(records)} processed ({saved} saved, {skipped} skipped)")
        
        return saved
