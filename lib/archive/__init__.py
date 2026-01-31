"""Archive slug discovery module.

Discovers booking engine slugs from web archives:
- Wayback Machine CDX API
- Common Crawl Index API
"""

from .discovery import ArchiveSlugDiscovery, BookingEnginePattern, DiscoveredSlug

__all__ = ["ArchiveSlugDiscovery", "BookingEnginePattern", "DiscoveredSlug"]
