from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, ConfigDict


class Job(BaseModel):
    """Job execution tracking model matching the database schema."""

    id: int

    # Job identification
    job_type: str
    hotel_id: Optional[int] = None

    # For export jobs
    city: Optional[str] = None
    state: Optional[str] = None
    export_type: Optional[str] = None

    # Queue info
    queue_name: Optional[str] = None
    message_id: Optional[str] = None
    attempt_number: int = 1

    # Execution
    worker_id: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None

    # Status (0=pending, 1=running, 2=completed, 3=failed, 4=retrying)
    status: int = 1
    error_message: Optional[str] = None
    error_stack: Optional[str] = None

    # Metadata
    input_params: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None

    # S3 log reference
    s3_log_path: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
