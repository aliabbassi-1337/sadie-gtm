"""
Base source handler - Abstract base for data source handlers.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Tuple, Optional, List
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Base configuration for source handlers."""

    use_cache: bool = Field(default=True, description="Whether to use cached files")
    cache_dir: Optional[str] = Field(
        default=None, description="Directory for caching files"
    )


class BaseSource(ABC):
    """
    Abstract base class for data source handlers.

    Handles fetching data from various sources (HTTP, S3, local filesystem).
    """

    def __init__(self, config: Optional[SourceConfig] = None):
        self.config = config or SourceConfig()

    @abstractmethod
    async def list_files(self, pattern: str = "*") -> List[str]:
        """
        List available files matching the pattern.

        Args:
            pattern: Glob pattern to match files

        Returns:
            List of file identifiers (paths, keys, URLs, etc.)
        """
        pass

    @abstractmethod
    async def fetch_file(self, identifier: str) -> bytes:
        """
        Fetch a single file by its identifier.

        Args:
            identifier: File path, S3 key, URL, etc.

        Returns:
            Raw file contents as bytes
        """
        pass

    async def fetch_all(self, pattern: str = "*") -> AsyncIterator[Tuple[str, bytes]]:
        """
        Fetch all files matching the pattern.

        Yields:
            Tuples of (identifier, content)
        """
        files = await self.list_files(pattern)
        for file_id in files:
            content = await self.fetch_file(file_id)
            yield file_id, content
