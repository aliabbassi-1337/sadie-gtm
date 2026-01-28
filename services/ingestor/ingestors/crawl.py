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
from services.ingestor.sources.s3 import S3Source
from services.ingestor import repo


# Known booking engines for detection
KNOWN_ENGINES = ["cloudbeds", "mews", "rms", "siteminder"]


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
        self._engine_name: Optional[str] = None
    
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
            for engine in KNOWN_ENGINES:
                if engine in filename_lower:
                    ingestors.append(cls(engine=engine, file_path=str(txt_file)))
                    break
        
        return ingestors
    
    @classmethod
    async def from_s3(
        cls,
        bucket: str,
        prefix: str = "",
        cache_dir: Optional[str] = None,
    ) -> List["CrawlIngestor"]:
        """
        Create ingestors for all crawl files in an S3 bucket.
        
        Priority for each engine:
        1. {engine}_deduped.txt (if exists)
        2. {engine}.txt (base file)
        3. Skip {engine}_commoncrawl.txt (raw, not deduped)
        
        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix (e.g., "crawl-data/")
            cache_dir: Local directory to cache downloaded files
        
        Returns:
            List of CrawlIngestor instances, one per engine file found
        """
        source = S3Source(bucket=bucket, prefix=prefix, cache_dir=cache_dir)
        
        # List all txt files
        all_keys = await source.list_files("*.txt")
        
        # Build a map of engine -> best file (prefer deduped)
        engine_files: dict[str, str] = {}
        
        for key in all_keys:
            filename = key.split("/")[-1].lower()
            
            # Skip commoncrawl (raw) files
            if "_commoncrawl" in filename:
                continue
            
            # Detect engine from filename
            for engine in KNOWN_ENGINES:
                if filename.startswith(engine):
                    # Prefer _deduped version
                    is_deduped = "_deduped" in filename
                    current = engine_files.get(engine)
                    
                    if current is None:
                        engine_files[engine] = key
                    elif is_deduped and "_deduped" not in current:
                        # Replace with deduped version
                        engine_files[engine] = key
                    break
        
        # Create ingestors for each engine
        ingestors = []
        for engine, key in engine_files.items():
            content = await source.fetch_file(key)
            slugs = content.decode("utf-8").strip().split("\n")
            slugs = list(set(s.strip().lower() for s in slugs if s.strip()))
            
            filename = key.split("/")[-1]
            logger.info(f"Loaded {len(slugs)} unique slugs for {engine} from {filename}")
            ingestors.append(cls(engine=engine, slugs=slugs))
        
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
    
    async def _get_engine_display_name(self) -> str:
        """
        Get the display name for this engine from the database.
        Falls back to title case if not found.
        """
        if self._engine_name:
            return self._engine_name
        
        # Try to find existing engine in DB
        engine = await repo.get_booking_engine_by_name(self.engine.title())
        if engine:
            self._engine_name = engine["name"]
            return self._engine_name
        
        # Known display names as fallback
        fallbacks = {
            "cloudbeds": "Cloudbeds",
            "mews": "Mews",
            "rms": "RMS Cloud",
            "siteminder": "SiteMinder",
        }
        self._engine_name = fallbacks.get(self.engine, self.engine.title())
        return self._engine_name
    
    async def _get_or_create_booking_engine_id(self) -> Optional[int]:
        """Get or create the booking engine ID from the database."""
        if self._booking_engine_id:
            return self._booking_engine_id
        
        engine_name = await self._get_engine_display_name()
        self._booking_engine_id = await repo.get_or_create_booking_engine(
            name=engine_name, tier=1
        )
        return self._booking_engine_id
    
    async def _batch_save(self, records: List[CrawledHotel], batch_size: int = 500) -> int:
        """
        Batch insert crawled hotels with booking engine linking.
        
        Uses true batch operations:
        1. Bulk check which booking_urls already exist
        2. Filter to new records only
        3. Bulk insert via executemany (single query per record, but batched)
        """
        if not records:
            return 0
        
        engine_id = await self._get_or_create_booking_engine_id()
        if not engine_id:
            logger.error(f"Failed to get booking engine ID for {self.engine}")
            return 0
        
        total_saved = 0
        total_skipped = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Step 1: Bulk check existing booking URLs
            booking_urls = [r.booking_url for r in batch]
            existing_urls = await repo.get_existing_booking_urls(booking_urls)
            
            # Step 2: Filter to new records only
            new_records = [r for r in batch if r.booking_url not in existing_urls]
            skipped = len(batch) - len(new_records)
            total_skipped += skipped
            
            if not new_records:
                logger.info(f"  Batch {i // batch_size + 1}: {i + len(batch)}/{len(records)} - all {skipped} skipped (duplicates)")
                continue
            
            # Step 3: Bulk insert via executemany
            # Format: (name, source, external_id, external_id_type, booking_engine_id, booking_url, slug, detection_method)
            tuples = [
                (
                    r.name,
                    r.source,
                    r.external_id,
                    r.external_id_type,
                    engine_id,
                    r.booking_url,
                    r.slug,
                    r.detection_method,
                )
                for r in new_records
            ]
            
            try:
                await repo.batch_insert_crawled_hotels(tuples)
                total_saved += len(new_records)
            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
                total_skipped += len(new_records)
            
            logger.info(f"  Batch {i // batch_size + 1}: {i + len(batch)}/{len(records)} - {len(new_records)} saved, {skipped} skipped")
        
        return total_saved
