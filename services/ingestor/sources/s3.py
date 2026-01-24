"""
S3 source handler - Fetch data from AWS S3 buckets.
"""

from typing import List, Optional
from pathlib import Path
from fnmatch import fnmatch
import os
from pydantic import Field
from loguru import logger

from services.ingestor.sources.base import BaseSource, SourceConfig

# Optional S3 support - gracefully handle missing boto3
try:
    import aioboto3

    HAS_S3 = True
except ImportError:
    HAS_S3 = False


class S3SourceConfig(SourceConfig):
    """Configuration for S3 source handler."""

    bucket: str = Field(..., description="S3 bucket name")
    prefix: str = Field(default="", description="S3 key prefix")
    region: Optional[str] = Field(default=None, description="AWS region")


class S3Source(BaseSource):
    """
    Fetch data from AWS S3 buckets.

    Requires aioboto3 to be installed: pip install aioboto3
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: Optional[str] = None,
        cache_dir: Optional[str] = None,
        use_cache: bool = True,
    ):
        if not HAS_S3:
            raise ImportError(
                "S3 support requires aioboto3. Install with: pip install aioboto3"
            )

        config = S3SourceConfig(
            bucket=bucket,
            prefix=prefix,
            region=region,
            cache_dir=cache_dir,
            use_cache=use_cache,
        )
        super().__init__(config)
        self.config: S3SourceConfig = config

        if cache_dir:
            self._cache_path = Path(cache_dir)
            self._cache_path.mkdir(parents=True, exist_ok=True)
        else:
            self._cache_path = None

    async def list_files(self, pattern: str = "*.csv") -> List[str]:
        """
        List files in the S3 bucket matching the pattern.

        Args:
            pattern: Glob pattern to match files (e.g., "*.csv")

        Returns:
            List of S3 keys
        """
        session = aioboto3.Session()
        files = []

        async with session.client("s3", region_name=self.config.region) as s3:
            paginator = s3.get_paginator("list_objects_v2")

            async for page in paginator.paginate(
                Bucket=self.config.bucket, Prefix=self.config.prefix
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = key.split("/")[-1]
                    if fnmatch(filename, pattern):
                        files.append(key)

        logger.info(f"Found {len(files)} files matching '{pattern}' in s3://{self.config.bucket}/{self.config.prefix}")
        return files

    async def fetch_file(self, key: str) -> bytes:
        """
        Fetch a file from S3.

        Args:
            key: S3 object key

        Returns:
            File contents as bytes
        """
        filename = key.split("/")[-1]

        # Check cache first
        if self._cache_path and self.config.use_cache:
            cache_file = self._cache_path / filename
            if cache_file.exists():
                logger.info(f"  Using cached {filename}")
                return cache_file.read_bytes()

        # Fetch from S3
        session = aioboto3.Session()
        async with session.client("s3", region_name=self.config.region) as s3:
            response = await s3.get_object(Bucket=self.config.bucket, Key=key)
            content = await response["Body"].read()

            # Save to cache
            if self._cache_path:
                cache_file = self._cache_path / filename
                cache_file.write_bytes(content)
                logger.info(f"  Cached {filename}")

            return content
