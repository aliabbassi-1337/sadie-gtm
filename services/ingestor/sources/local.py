"""
Local filesystem source handler - Read data from local files.
"""

from typing import List, Optional
from pathlib import Path
from pydantic import Field
from loguru import logger

from services.ingestor.sources.base import BaseSource, SourceConfig


class LocalSourceConfig(SourceConfig):
    """Configuration for local filesystem source handler."""

    path: str = Field(..., description="Directory path to read from")
    encoding: str = Field(default="utf-8", description="File encoding")


class LocalSource(BaseSource):
    """
    Read data from local filesystem.

    Supports reading from directories with glob patterns.
    """

    def __init__(
        self,
        path: str,
        encoding: str = "utf-8",
    ):
        config = LocalSourceConfig(
            path=path,
            encoding=encoding,
            use_cache=False,  # No caching for local files
        )
        super().__init__(config)
        self.config: LocalSourceConfig = config
        self._base_path = Path(path)

    async def list_files(self, pattern: str = "*.csv") -> List[str]:
        """
        List files in the directory matching the pattern.

        Args:
            pattern: Glob pattern to match files (e.g., "*.csv", "**/*.csv")

        Returns:
            List of file paths
        """
        if not self._base_path.exists():
            logger.warning(f"Path does not exist: {self._base_path}")
            return []

        if self._base_path.is_file():
            return [str(self._base_path)]

        files = []
        for match in self._base_path.glob(pattern):
            if match.is_file():
                files.append(str(match))

        # Also try case-insensitive for common extensions
        if pattern.lower().endswith(".csv"):
            for match in self._base_path.glob(pattern.upper()):
                if match.is_file() and str(match) not in files:
                    files.append(str(match))

        logger.info(f"Found {len(files)} files matching '{pattern}' in {self._base_path}")
        return sorted(files)

    async def fetch_file(self, filepath: str) -> bytes:
        """
        Read a local file.

        Args:
            filepath: Path to the file

        Returns:
            File contents as bytes
        """
        path = Path(filepath)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # Try specified encoding first, fall back to latin-1 if needed
        try:
            return path.read_bytes()
        except Exception as e:
            logger.error(f"Error reading {filepath}: {e}")
            raise
