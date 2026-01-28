"""
Ingestor implementations.

Import this module to register all ingestors with the registry.
"""

from services.ingestor.ingestors.crawl import CrawlIngestor
from services.ingestor.ingestors.dbpr import DBPRIngestor
from services.ingestor.ingestors.texas import TexasIngestor
from services.ingestor.ingestors.generic_csv import GenericCSVIngestor

__all__ = [
    "CrawlIngestor",
    "DBPRIngestor",
    "TexasIngestor",
    "GenericCSVIngestor",
]
