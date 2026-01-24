"""Tests for HTTP source handler."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch, MagicMock

from services.ingestor.sources.http import HTTPSource


class TestHTTPSource:
    """Tests for HTTPSource handler."""

    @pytest.mark.no_db
    def test_init_with_urls(self):
        """Initialize with list of URLs."""
        source = HTTPSource(
            urls=["https://example.com/file1.csv", "https://example.com/file2.csv"],
            timeout=60.0,
        )

        assert len(source.config.urls) == 2
        assert source.config.timeout == 60.0

    @pytest.mark.no_db
    def test_init_with_cache_dir(self):
        """Initialize with cache directory."""
        with TemporaryDirectory() as tmpdir:
            source = HTTPSource(
                urls=["https://example.com/file.csv"],
                cache_dir=tmpdir,
                use_cache=True,
            )

            assert source._cache_path == Path(tmpdir)
            assert source.config.use_cache is True

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_returns_urls(self):
        """List files returns configured URLs."""
        source = HTTPSource(
            urls=["https://example.com/a.csv", "https://example.com/b.csv"],
        )

        files = await source.list_files()

        assert files == ["https://example.com/a.csv", "https://example.com/b.csv"]

    @pytest.mark.no_db
    def test_add_url(self):
        """Add URL to source."""
        source = HTTPSource(urls=[])
        source.add_url("https://example.com/new.csv")

        assert "https://example.com/new.csv" in source.config.urls

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_uses_cache(self):
        """Fetch file uses cache when available."""
        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_file = cache_dir / "cached.csv"
            cache_file.write_bytes(b"cached content")

            source = HTTPSource(
                urls=["https://example.com/cached.csv"],
                cache_dir=str(cache_dir),
                use_cache=True,
            )

            content = await source.fetch_file("https://example.com/cached.csv")

            assert content == b"cached content"

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_downloads_and_caches(self):
        """Fetch file downloads and caches when not in cache."""
        with TemporaryDirectory() as tmpdir:
            # Mock httpx client
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b"downloaded content"

            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client.get = AsyncMock(return_value=mock_response)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_class.return_value = mock_client

                source = HTTPSource(
                    urls=["https://example.com/download.csv"],
                    cache_dir=tmpdir,
                    use_cache=True,
                )

                content = await source.fetch_file("https://example.com/download.csv")

                assert content == b"downloaded content"
                # Check file was cached
                cache_file = Path(tmpdir) / "download.csv"
                assert cache_file.exists()
                assert cache_file.read_bytes() == b"downloaded content"

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_raises_on_error(self):
        """Fetch file raises error on HTTP failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock(
            side_effect=Exception("Not Found")
        )

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            source = HTTPSource(urls=["https://example.com/missing.csv"])

            with pytest.raises(Exception):
                await source.fetch_file("https://example.com/missing.csv")
