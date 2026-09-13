import pytest
from pydantic import ValidationError
from ingestion.schemas import TransactionPayload


def test_valid_transaction_schema():
    valid_data = {
        "transaction_id": "8f7e2d90-3b4c-4e89-a1b2-c3d4e5f6a7b8",
        "idempotency_key": "idemp_abc123456",
        "user_id": "usr_1001",
        "merchant_id": "merch_500",
        "amount": 149.99,
        "currency": "INR",
        "payment_method": "UPI",
        "status": "SUCCESS",
        "event_timestamp": "2026-09-13T10:00:00Z",
    }
    payload = TransactionPayload(**valid_data)
    assert payload.amount == 149.99
    assert payload.currency == "INR"


def test_invalid_currency_raises_validation_error():
    invalid_data = {
        "transaction_id": "8f7e2d90-3b4c-4e89-a1b2-c3d4e5f6a7b8",
        "idempotency_key": "idemp_abc123456",
        "user_id": "usr_1001",
        "merchant_id": "merch_500",
        "amount": 50.0,
        "currency": "BITCOIN",  # Invalid currency not in Enum
        "payment_method": "UPI",
        "status": "SUCCESS",
        "event_timestamp": "2026-09-13T10:00:00Z",
    }
    with pytest.raises(ValidationError):
        TransactionPayload(**invalid_data)