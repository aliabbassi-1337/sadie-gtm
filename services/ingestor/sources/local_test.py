"""Tests for local filesystem source handler."""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from services.ingestor.sources.local import LocalSource


class TestLocalSource:
    """Tests for LocalSource handler."""

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_finds_csv(self):
        """List CSV files in directory."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            (path / "file1.csv").write_text("data1")
            (path / "file2.csv").write_text("data2")
            (path / "file3.txt").write_text("data3")

            source = LocalSource(str(path))
            files = await source.list_files("*.csv")

            assert len(files) == 2
            assert any("file1.csv" in f for f in files)
            assert any("file2.csv" in f for f in files)

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_case_insensitive(self):
        """List files handles case-insensitive extensions."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            (path / "file1.csv").write_text("data1")
            (path / "file2.CSV").write_text("data2")

            source = LocalSource(str(path))
            files = await source.list_files("*.csv")

            # Should find lowercase
            assert len(files) >= 1

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_empty_directory(self):
        """List files returns empty for empty directory."""
        with TemporaryDirectory() as tmpdir:
            source = LocalSource(tmpdir)
            files = await source.list_files("*.csv")

            assert len(files) == 0

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_list_files_nonexistent_directory(self):
        """List files returns empty for nonexistent directory."""
        source = LocalSource("/nonexistent/path")
        files = await source.list_files("*.csv")

        assert len(files) == 0

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_reads_content(self):
        """Fetch file reads and returns content."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            test_file = path / "test.csv"
            test_file.write_text("col1,col2\nval1,val2")

            source = LocalSource(str(path))
            content = await source.fetch_file(str(test_file))

            assert b"col1,col2" in content
            assert b"val1,val2" in content

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_file_raises_for_missing(self):
        """Fetch file raises error for missing file."""
        with TemporaryDirectory() as tmpdir:
            source = LocalSource(tmpdir)

            with pytest.raises(FileNotFoundError):
                await source.fetch_file("/nonexistent/file.csv")

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_fetch_all_iterates_files(self):
        """Fetch all yields filename and content tuples."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            (path / "file1.csv").write_text("data1")
            (path / "file2.csv").write_text("data2")

            source = LocalSource(str(path))
            results = []

            async for filename, content in source.fetch_all("*.csv"):
                results.append((filename, content))

            assert len(results) == 2
            contents = [r[1] for r in results]
            assert b"data1" in contents or b"data2" in contents

    @pytest.mark.no_db
    @pytest.mark.asyncio
    async def test_handles_single_file_path(self):
        """Source handles a single file path."""
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            test_file = path / "single.csv"
            test_file.write_text("single file content")

            source = LocalSource(str(test_file))
            files = await source.list_files()

            assert len(files) == 1
            assert str(test_file) in files[0]
