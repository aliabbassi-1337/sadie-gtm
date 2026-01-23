"""S3 client for file upload operations."""

import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from loguru import logger


def get_s3_client():
    """Get S3 client using environment credentials."""
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
    )


def get_bucket_name() -> str:
    """Get the S3 bucket name from environment."""
    bucket = os.getenv("S3_BUCKET_NAME", "sadie-gtm")
    return bucket


def upload_file(
    local_path: str,
    s3_key: str,
    bucket: Optional[str] = None,
    content_type: Optional[str] = None,
) -> str:
    """Upload a file to S3.

    Args:
        local_path: Path to local file
        s3_key: S3 object key (path in bucket)
        bucket: S3 bucket name (defaults to S3_BUCKET_NAME env var)
        content_type: Optional content type (auto-detected if not provided)

    Returns:
        S3 URI (s3://bucket/key)

    Raises:
        ClientError: If upload fails
    """
    client = get_s3_client()
    bucket = bucket or get_bucket_name()

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    elif s3_key.endswith(".xlsx"):
        extra_args["ContentType"] = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    try:
        client.upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs=extra_args if extra_args else None,
        )
        s3_uri = f"s3://{bucket}/{s3_key}"
        logger.info(f"Uploaded {local_path} to {s3_uri}")
        return s3_uri
    except ClientError as e:
        logger.error(f"Failed to upload {local_path} to s3://{bucket}/{s3_key}: {e}")
        raise


def file_exists(s3_key: str, bucket: Optional[str] = None) -> bool:
    """Check if a file exists in S3.

    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (defaults to S3_BUCKET_NAME env var)

    Returns:
        True if file exists, False otherwise
    """
    client = get_s3_client()
    bucket = bucket or get_bucket_name()

    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def delete_file(s3_key: str, bucket: Optional[str] = None) -> bool:
    """Delete a file from S3.

    Args:
        s3_key: S3 object key
        bucket: S3 bucket name (defaults to S3_BUCKET_NAME env var)

    Returns:
        True if deleted (or didn't exist), False on error
    """
    client = get_s3_client()
    bucket = bucket or get_bucket_name()

    try:
        client.delete_object(Bucket=bucket, Key=s3_key)
        logger.info(f"Deleted s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to delete s3://{bucket}/{s3_key}: {e}")
        return False
