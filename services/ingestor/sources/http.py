"""
HTTP source handler - Fetch data from HTTP/HTTPS URLs.
"""

from typing import List, Optional
from pathlib import Path
import httpx
from pydantic import Field
from loguru import logger

from services.ingestor.sources.base import BaseSource, SourceConfig


class HTTPSourceConfig(SourceConfig):
    """Configuration for HTTP source handler."""

    urls: List[str] = Field(default_factory=list, description="URLs to fetch")
    timeout: float = Field(default=120.0, description="Request timeout in seconds")
    headers: dict = Field(default_factory=dict, description="HTTP headers to send")


class HTTPSource(BaseSource):
    """
    Fetch data from HTTP/HTTPS URLs.

    Supports caching to avoid repeated downloads.
    """

    def __init__(
        self,
        urls: Optional[List[str]] = None,
        timeout: float = 120.0,
        cache_dir: Optional[str] = None,
        use_cache: bool = True,
        headers: Optional[dict] = None,
    ):
        config = HTTPSourceConfig(
            urls=urls or [],
            timeout=timeout,
            cache_dir=cache_dir,
            use_cache=use_cache,
            headers=headers or {},
        )
        super().__init__(config)
        self.config: HTTPSourceConfig = config

        if cache_dir:
            self._cache_path = Path(cache_dir)
            self._cache_path.mkdir(parents=True, exist_ok=True)
        else:
            self._cache_path = None

    async def list_files(self, pattern: str = "*") -> List[str]:
        """Return configured URLs as file list."""
        return self.config.urls

    async def fetch_file(self, url: str) -> bytes:
        """
        Fetch a file from URL with optional caching.

        Args:
            url: URL to fetch

        Returns:
            File contents as bytes
        """
        # Check cache first
        if self._cache_path and self.config.use_cache:
            filename = url.split("/")[-1]
            cache_file = self._cache_path / filename

            if cache_file.exists():
                logger.info(f"  Using cached {filename}")
                return cache_file.read_bytes()

        # Fetch from URL
        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.get(url, headers=self.config.headers)
            response.raise_for_status()
            content = response.content

            # Save to cache
            if self._cache_path:
                filename = url.split("/")[-1]
                cache_file = self._cache_path / filename
                cache_file.write_bytes(content)
                logger.info(f"  Cached {filename}")

            return content

    def add_url(self, url: str) -> None:
        """Add a URL to fetch."""
        self.config.urls.append(url)
