"""Tests for ingestor logging module."""

import gzip
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock
from loguru import logger

from services.ingestor.logging import IngestLogger, capture_ingest_logs


class TestIngestLogger:
    """Tests for IngestLogger class."""

    @pytest.mark.no_db
    def test_captures_logs(self):
        """Logger captures log messages."""
        with IngestLogger("test", s3_bucket=None) as log:
            logger.info("Test message 1")
            logger.info("Test message 2")

        content = log._log_buffer.getvalue()
        assert "Test message 1" in content
        assert "Test message 2" in content

    @pytest.mark.no_db
    def test_includes_start_end_markers(self):
        """Logger includes start and end markers."""
        with IngestLogger("test_source", s3_bucket=None) as log:
            logger.info("Work in progress")

        content = log._log_buffer.getvalue()
        assert "=== Ingestion started: test_source ===" in content
        assert "=== Ingestion completed: test_source ===" in content

    @pytest.mark.no_db
    def test_includes_timestamps(self):
        """Logger includes timestamps."""
        with IngestLogger("test", s3_bucket=None) as log:
            logger.info("Test")

        content = log._log_buffer.getvalue()
        assert "Start time:" in content
        assert "End time:" in content
        assert "Duration:" in content

    @pytest.mark.no_db
    def test_saves_local_backup(self):
        """Logger saves local backup when configured."""
        with TemporaryDirectory() as tmpdir:
            with IngestLogger("test", s3_bucket=None, local_backup_dir=tmpdir) as log:
                logger.info("Test message")

            # Check that a .log.gz file was created
            files = list(Path(tmpdir).glob("*.log.gz"))
            assert len(files) == 1

            # Verify content is compressed
            content = gzip.decompress(files[0].read_bytes()).decode()
            assert "Test message" in content

    @pytest.mark.no_db
    def test_uploads_to_s3(self):
        """Logger uploads to S3 when bucket is configured."""
        mock_s3 = MagicMock()

        with patch("services.ingestor.logging.HAS_BOTO3", True):
            with patch("services.ingestor.logging.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3

                with IngestLogger("dbpr", s3_bucket="test-bucket") as log:
                    logger.info("Test message")

                # Verify S3 put_object was called
                mock_s3.put_object.assert_called_once()
                call_kwargs = mock_s3.put_object.call_args[1]
                assert call_kwargs["Bucket"] == "test-bucket"
                assert call_kwargs["Key"].startswith("ingest-logs/dbpr_")
                assert call_kwargs["Key"].endswith(".log.gz")

    @pytest.mark.no_db
    def test_custom_s3_prefix(self):
        """Logger uses custom S3 prefix."""
        mock_s3 = MagicMock()

        with patch("services.ingestor.logging.HAS_BOTO3", True):
            with patch("services.ingestor.logging.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3

                with IngestLogger(
                    "texas",
                    s3_bucket="test-bucket",
                    s3_prefix="custom/logs/",
                ) as log:
                    logger.info("Test")

                call_kwargs = mock_s3.put_object.call_args[1]
                assert call_kwargs["Key"].startswith("custom/logs/texas_")

    @pytest.mark.no_db
    def test_logs_exception_on_error(self):
        """Logger captures exception info when error occurs."""
        try:
            with IngestLogger("test", s3_bucket=None) as log:
                logger.info("Before error")
                raise ValueError("Test error")
        except ValueError:
            pass

        content = log._log_buffer.getvalue()
        assert "Before error" in content
        assert "Ingestion failed with error" in content
        assert "Test error" in content

    @pytest.mark.no_db
    def test_uses_env_bucket(self):
        """Logger uses INGEST_LOG_BUCKET env var."""
        mock_s3 = MagicMock()

        with patch.dict("os.environ", {"INGEST_LOG_BUCKET": "env-bucket"}):
            with patch("services.ingestor.logging.HAS_BOTO3", True):
                with patch("services.ingestor.logging.boto3") as mock_boto3:
                    mock_boto3.client.return_value = mock_s3

                    with IngestLogger("test") as log:
                        logger.info("Test")

                    call_kwargs = mock_s3.put_object.call_args[1]
                    assert call_kwargs["Bucket"] == "env-bucket"


class TestCaptureIngestLogs:
    """Tests for capture_ingest_logs context manager."""

    @pytest.mark.no_db
    def test_context_manager_works(self):
        """Context manager captures logs."""
        with capture_ingest_logs("test", s3_bucket=None) as log:
            logger.info("Captured message")

        content = log._log_buffer.getvalue()
        assert "Captured message" in content

    @pytest.mark.no_db
    def test_passes_all_options(self):
        """Context manager passes all options."""
        with TemporaryDirectory() as tmpdir:
            with capture_ingest_logs(
                "test",
                s3_bucket=None,
                s3_prefix="custom/",
                local_backup_dir=tmpdir,
            ) as log:
                logger.info("Test")

            files = list(Path(tmpdir).glob("*.log.gz"))
            assert len(files) == 1
