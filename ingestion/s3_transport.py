import io
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("S3Transport")


class S3TransportEngine:
    """Manages resilient batch transport to S3 Bronze layer

    with backoff retries and temporal partitioning.
    """

    def __init__(
        self,
        bucket_name: str = None,
        max_retries: int = 4,
        base_backoff_seconds: float = 1.0,
    ):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME")
        if not self.bucket_name:
            raise ValueError(
                "S3_BUCKET_NAME is not set in environment or constructor."
            )

        self.max_retries = max_retries
        self.base_backoff = base_backoff_seconds
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
        )

    def _generate_partition_path(self, target_dt: datetime) -> str:
        """Constructs Hive-style directory path based on UTC timestamp."""
        year = target_dt.strftime("%Y")
        month = target_dt.strftime("%m")
        day = target_dt.strftime("%d")
        hour = target_dt.strftime("%H")
        file_id = uuid.uuid4().hex[:8]
        return (
            f"bronze/year={year}/month={month}/day={day}/hour={hour}/"
            f"transactions_batch_{file_id}.json"
        )

    def _serialize_to_ndjson(self, records: List[Dict[str, Any]]) -> bytes:
        """Serializes list of dictionaries to newline-delimited JSON bytes."""
        buffer = io.StringIO()
        for item in records:
            buffer.write(json.dumps(item) + "\n")
        return buffer.getvalue().encode("utf-8")

    def upload_batch(
        self, records: List[Dict[str, Any]], custom_key: str = None
    ) -> str:
        """Uploads records with exponential backoff and randomized jitter."""
        if not records:
            logger.warning("Empty records list passed to upload_batch. Skipping.")
            return ""

        payload_bytes = self._serialize_to_ndjson(records)
        s3_key = custom_key or self._generate_partition_path(
            datetime.now(timezone.utc)
        )

        attempt = 0
        while attempt < self.max_retries:
            try:
                logger.info(
                    f"Attempting upload of {len(records)} records to "
                    f"s3://{self.bucket_name}/{s3_key} (Attempt {attempt + 1})"
                )
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=payload_bytes,
                    ContentType="application/x-ndjson",
                )
                logger.info(f"Successfully uploaded batch to S3: {s3_key}")
                return s3_key

            except (ClientError, EndpointConnectionError) as error:
                attempt += 1
                if attempt >= self.max_retries:
                    logger.error(
                        f"Failed to upload to S3 after {self.max_retries} attempts: {error}"
                    )
                    raise RuntimeError(
                        f"Critical upload failure to S3: {error}"
                    ) from error

                # Exponential backoff with Full Jitter: wait = random_between(0, base * 2^attempt)
                wait_time = random.uniform(0, self.base_backoff * (2**attempt))
                logger.warning(
                    f"Upload transient error encountered ({error}). "
                    f"Backing off for {wait_time:.2f}s before retry..."
                )
                time.sleep(wait_time)

        return ""