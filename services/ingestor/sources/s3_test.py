"""Tests for S3 source handler."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch, MagicMock
import sys


# Check if aioboto3 is available
try:
    import aioboto3
    HAS_AIOBOTO3 = True
except ImportError:
    HAS_AIOBOTO3 = False


class TestS3SourceConfig:
    """Tests for S3SourceConfig."""

    @pytest.mark.no_db
    def test_config_requires_bucket(self):
        """Config requires bucket field."""
        from services.ingestor.sources.s3 import S3SourceConfig

        with pytest.raises(Exception):  # Pydantic validation error
            S3SourceConfig()

    @pytest.mark.no_db
    def test_config_defaults(self):
        """Config has correct defaults."""
        from services.ingestor.sources.s3 import S3SourceConfig

        config = S3SourceConfig(bucket="test")

        assert config.bucket == "test"
        assert config.prefix == ""
        assert config.region is None
        assert config.use_cache is True


class TestS3SourceNoAioboto3:
    """Tests for S3Source when aioboto3 is not available."""

    @pytest.mark.no_db
    def test_init_without_aioboto3_raises(self):
        """Initialization without aioboto3 raises ImportError."""
        # Temporarily make HAS_S3 False
        import services.ingestor.sources.s3 as s3_module
        original_has_s3 = s3_module.HAS_S3

        try:
            s3_module.HAS_S3 = False
            from services.ingestor.sources.s3 import S3Source

            with pytest.raises(ImportError, match="aioboto3"):
                S3Source(bucket="test-bucket")
        finally:
            s3_module.HAS_S3 = original_has_s3


@pytest.mark.skipif(not HAS_AIOBOTO3, reason="aioboto3 not installed")
class TestS3Source:
    """Tests for S3Source handler when aioboto3 is available."""

    @pytest.mark.no_db
    def test_init_with_aioboto3(self):
        """Initialize with aioboto3 available."""
        from services.ingestor.sources.s3 import S3Source

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            region="us-east-1",
        )

        assert source.config.bucket == "test-bucket"
        assert source.config.prefix == "data/"
        assert source.config.region == "us-east-1"

    @pytest.mark.no_db
    def test_init_with_cache_dir(self):
        """Initialize creates cache directory."""
        from services.ingestor.sources.s3 import S3Source

        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            source = S3Source(
                bucket="test-bucket",
                cache_dir=str(cache_dir),
                use_cache=True,
            )

            assert source._cache_path == cache_dir
            assert cache_dir.exists()

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_uses_cache(self):
        """Fetch file uses cache when available."""
        from services.ingestor.sources.s3 import S3Source

        with TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_file = cache_dir / "cached.csv"
            cache_file.write_bytes(b"cached content")

            source = S3Source(
                bucket="test-bucket",
                cache_dir=str(cache_dir),
                use_cache=True,
            )

            content = await source.fetch_file("data/cached.csv")

            assert content == b"cached content"

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_returns_matching_keys(self):
        """List files returns keys matching pattern."""
        from services.ingestor.sources.s3 import S3Source

        # Create mock paginator
        mock_page = {
            "Contents": [
                {"Key": "data/file1.csv"},
                {"Key": "data/file2.csv"},
                {"Key": "data/readme.txt"},
            ]
        }

        class MockPaginator:
            def paginate(self, **kwargs):
                return self

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not hasattr(self, "_returned"):
                    self._returned = True
                    return mock_page
                raise StopAsyncIteration

        mock_s3 = AsyncMock()
        mock_s3.get_paginator = MagicMock(return_value=MockPaginator())

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__ = AsyncMock(return_value=mock_s3)
        mock_client_context.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.client = MagicMock(return_value=mock_client_context)

        with patch("aioboto3.Session", return_value=mock_session):
            source = S3Source(bucket="test-bucket", prefix="data/")
            files = await source.list_files("*.csv")

            assert len(files) == 2
            assert "data/file1.csv" in files
            assert "data/file2.csv" in files
            assert "data/readme.txt" not in files

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_downloads_and_caches(self):
        """Fetch file downloads from S3 and caches."""
        from services.ingestor.sources.s3 import S3Source

        # Mock S3 response
        mock_body = AsyncMock()
        mock_body.read = AsyncMock(return_value=b"s3 content")

        mock_s3 = AsyncMock()
        mock_s3.get_object = AsyncMock(return_value={"Body": mock_body})

        mock_client_context = AsyncMock()
        mock_client_context.__aenter__ = AsyncMock(return_value=mock_s3)
        mock_client_context.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.client = MagicMock(return_value=mock_client_context)

        with TemporaryDirectory() as tmpdir:
            with patch("aioboto3.Session", return_value=mock_session):
                source = S3Source(
                    bucket="test-bucket",
                    cache_dir=tmpdir,
                    use_cache=True,
                )

                content = await source.fetch_file("data/new.csv")

                assert content == b"s3 content"
                # Check file was cached
                cache_file = Path(tmpdir) / "new.csv"
                assert cache_file.exists()
                assert cache_file.read_bytes() == b"s3 content"
