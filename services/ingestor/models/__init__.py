"""
Ingestor data models.
"""

from services.ingestor.models.base import BaseRecord, IngestStats
from services.ingestor.models.dbpr import DBPRLicense
from services.ingestor.models.texas import TexasHotel

__all__ = [
    "BaseRecord",
    "IngestStats",
    "DBPRLicense",
    "TexasHotel",
]
