"""
Data source handlers for ingestors.
"""

from services.ingestor.sources.base import BaseSource
from services.ingestor.sources.http import HTTPSource
from services.ingestor.sources.s3 import S3Source
from services.ingestor.sources.local import LocalSource

__all__ = [
    "BaseSource",
    "HTTPSource",
    "S3Source",
    "LocalSource",
]
