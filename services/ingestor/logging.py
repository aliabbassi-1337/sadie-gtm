"""
Ingestor logging - Capture, compress, and upload ingestion logs to S3.
"""

import gzip
import io
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from loguru import logger

# Optional S3 support
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


class IngestLogger:
    """
    Captures logs during ingestion and uploads to S3.

    Usage:
        with IngestLogger("dbpr") as log:
            # do ingestion work
            logger.info("Processing...")
        # Log is automatically compressed and uploaded to S3
    """

    def __init__(
        self,
        source_name: str,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "ingest-logs/",
        local_backup_dir: Optional[str] = None,
    ):
        """
        Initialize the ingest logger.

        Args:
            source_name: Name of the data source (e.g., 'dbpr', 'texas')
            s3_bucket: S3 bucket for log uploads (defaults to INGEST_LOG_BUCKET env var)
            s3_prefix: S3 key prefix for logs
            local_backup_dir: Local directory for log backup (optional)
        """
        self.source_name = source_name
        self.s3_bucket = s3_bucket or os.environ.get("INGEST_LOG_BUCKET")
        self.s3_prefix = s3_prefix
        self.local_backup_dir = local_backup_dir

        self._log_buffer = io.StringIO()
        self._handler_id: Optional[int] = None
        self._start_time: Optional[datetime] = None

    def __enter__(self) -> "IngestLogger":
        """Start capturing logs."""
        self._start_time = datetime.utcnow()

        # Add a custom sink to capture logs
        self._handler_id = logger.add(
            self._log_buffer,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
            level="DEBUG",
        )

        logger.info(f"=== Ingestion started: {self.source_name} ===")
        logger.info(f"Start time: {self._start_time.isoformat()}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop capturing and upload logs."""
        end_time = datetime.utcnow()
        duration = end_time - self._start_time

        if exc_type:
            logger.error(f"Ingestion failed with error: {exc_val}")

        logger.info(f"End time: {end_time.isoformat()}")
        logger.info(f"Duration: {duration}")
        logger.info(f"=== Ingestion completed: {self.source_name} ===")

        # Remove the handler
        if self._handler_id is not None:
            logger.remove(self._handler_id)

        # Get log content
        log_content = self._log_buffer.getvalue()

        # Compress and upload
        try:
            self._save_log(log_content, end_time)
        except Exception as e:
            logger.error(f"Failed to save ingestion log: {e}")

        return False  # Don't suppress exceptions

    def _save_log(self, content: str, timestamp: datetime) -> Optional[str]:
        """Compress and save the log file."""
        # Generate filename
        date_str = timestamp.strftime("%Y-%m-%d")
        time_str = timestamp.strftime("%H%M%S")
        filename = f"{self.source_name}_{date_str}_{time_str}.log.gz"

        # Compress
        compressed = gzip.compress(content.encode("utf-8"))

        s3_key = None

        # Upload to S3
        if self.s3_bucket and HAS_BOTO3:
            try:
                s3_key = self._upload_to_s3(compressed, filename)
                logger.info(f"Log uploaded to s3://{self.s3_bucket}/{s3_key}")
            except Exception as e:
                logger.error(f"S3 upload failed: {e}")

        # Save local backup if configured
        if self.local_backup_dir:
            try:
                local_path = self._save_local(compressed, filename)
                logger.info(f"Log saved locally: {local_path}")
            except Exception as e:
                logger.error(f"Local save failed: {e}")

        return s3_key

    def _upload_to_s3(self, content: bytes, filename: str) -> str:
        """Upload compressed log to S3."""
        if not HAS_BOTO3:
            raise ImportError("boto3 is required for S3 uploads")

        s3 = boto3.client("s3")
        key = f"{self.s3_prefix}{filename}"

        s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=content,
            ContentType="application/gzip",
            ContentEncoding="gzip",
        )

        return key

    def _save_local(self, content: bytes, filename: str) -> Path:
        """Save compressed log locally."""
        backup_dir = Path(self.local_backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)

        filepath = backup_dir / filename
        filepath.write_bytes(content)

        return filepath


@contextmanager
def capture_ingest_logs(
    source_name: str,
    s3_bucket: Optional[str] = None,
    s3_prefix: str = "ingest-logs/",
    local_backup_dir: Optional[str] = None,
):
    """
    Context manager to capture ingestion logs.

    Usage:
        with capture_ingest_logs("dbpr") as log:
            # do work
            logger.info("Processing...")
    """
    ingest_logger = IngestLogger(
        source_name=source_name,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        local_backup_dir=local_backup_dir,
    )

    with ingest_logger as log:
        yield log
