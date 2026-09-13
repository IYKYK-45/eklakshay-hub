from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import EndpointConnectionError
from ingestion.s3_transport import S3TransportEngine


@patch.dict(
    "os.environ",
    {
        "S3_BUCKET_NAME": "dummy-test-bucket",
        "AWS_ACCESS_KEY_ID": "mock_key",
        "AWS_SECRET_ACCESS_KEY": "mock_secret",
        "AWS_DEFAULT_REGION": "ap-south-1",
    },
)
def test_partition_path_generation():
    engine = S3TransportEngine()
    test_dt = datetime(2026, 9, 13, 11, 0, 0, tzinfo=timezone.utc)
    path = engine._generate_partition_path(test_dt)

    assert (
        path.startswith("bronze/year=2026/month=09/day=13/hour=11/") is True
    )
    assert path.endswith(".json") is True


@patch.dict(
    "os.environ",
    {
        "S3_BUCKET_NAME": "dummy-test-bucket",
        "AWS_ACCESS_KEY_ID": "mock_key",
        "AWS_SECRET_ACCESS_KEY": "mock_secret",
        "AWS_DEFAULT_REGION": "ap-south-1",
    },
)
def test_ndjson_serialization():
    engine = S3TransportEngine()
    sample_records = [{"id": 1, "amount": 10.5}, {"id": 2, "amount": 99.0}]
    raw_bytes = engine._serialize_to_ndjson(sample_records)
    lines = raw_bytes.decode("utf-8").strip().split("\n")

    assert len(lines) == 2
    assert '"id": 1' in lines[0]
    assert '"id": 2' in lines[1]


@patch.dict(
    "os.environ",
    {
        "S3_BUCKET_NAME": "dummy-test-bucket",
        "AWS_ACCESS_KEY_ID": "mock_key",
        "AWS_SECRET_ACCESS_KEY": "mock_secret",
        "AWS_DEFAULT_REGION": "ap-south-1",
    },
)
@patch("boto3.client")
def test_upload_batch_retry_on_failure(mock_boto_client):
    # Mock S3 client to fail twice with connection errors, then succeed on 3rd attempt
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = [
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
        {"ResponseMetadata": {"HTTPStatusCode": 200}},
    ]
    mock_boto_client.return_value = mock_s3

    engine = S3TransportEngine(max_retries=3, base_backoff_seconds=0.01)
    uploaded_key = engine.upload_batch([{"test": "payload"}])

    assert mock_s3.put_object.call_count == 3
    assert uploaded_key.startswith("bronze/year=") is True