import pytest
from spark.transformations import (
    TRANSACTION_SCHEMA,
    validate_and_route_records,
    deduplicate_and_enrich_silver,
)


def test_schema_routing_valid_and_quarantine(spark):
    """Validates that valid rows route to clean and corrupted rows route to quarantine with tags."""
    test_rows = [
        # 1. Clean valid transaction
        (
            "tx_101",
            "idemp_101",
            "usr_1",
            "merch_1",
            250.0,
            "INR",
            "UPI",
            "SUCCESS",
            "2026-09-13T10:00:00",
            "127.0.0.1",
        ),
        # 2. Bad amount: Negative value
        (
            "tx_102",
            "idemp_102",
            "usr_2",
            "merch_2",
            -50.0,
            "INR",
            "UPI",
            "SUCCESS",
            "2026-09-13T10:01:00",
            "127.0.0.1",
        ),
        # 3. Bad currency: Unsupported crypto
        (
            "tx_103",
            "idemp_103",
            "usr_3",
            "merch_3",
            100.0,
            "BTC",
            "CREDIT_CARD",
            "SUCCESS",
            "2026-09-13T10:02:00",
            "127.0.0.1",
        ),
        # 4. Bad timestamp: Malformed string
        (
            "tx_104",
            "idemp_104",
            "usr_4",
            "merch_4",
            75.0,
            "USD",
            "DEBIT_CARD",
            "SUCCESS",
            "INVALID_TIMESTAMP_FORMAT_2026",
            "127.0.0.1",
        ),
        # 5. Bad ID: Missing / None
        (
            None,
            "idemp_105",
            "usr_5",
            "merch_5",
            120.0,
            "EUR",
            "NET_BANKING",
            "SUCCESS",
            "2026-09-13T10:03:00",
            "127.0.0.1",
        ),
    ]

    raw_df = spark.createDataFrame(test_rows, schema=TRANSACTION_SCHEMA)
    clean_df, quarantine_df = validate_and_route_records(raw_df)

    # 1 valid row, 4 quarantined rows
    assert clean_df.count() == 1
    assert quarantine_df.count() == 4

    # Check that reasons are tagged accurately
    reasons = [r["quarantine_reason"] for r in quarantine_df.collect()]
    assert "INVALID_OR_NEGATIVE_AMOUNT" in reasons
    assert "UNSUPPORTED_CURRENCY" in reasons
    assert "INVALID_TIMESTAMP_FORMAT" in reasons
    assert "MISSING_OR_EMPTY_TRANSACTION_ID" in reasons


def test_deduplicate_keeps_latest_record(spark):
    """Ensures deduplication collapses duplicates and retains the most recent event timestamp."""
    duplicate_rows = [
        (
            "tx_dup_old",
            "shared_key_1",
            "usr_1",
            "merch_1",
            500.0,
            "INR",
            "UPI",
            "SUCCESS",
            "2026-09-13T08:00:00",
            "127.0.0.1",
        ),
        (
            "tx_dup_new",
            "shared_key_1",
            "usr_1",
            "merch_1",
            500.0,
            "INR",
            "UPI",
            "SUCCESS",
            "2026-09-13T09:30:00",  # Later timestamp
            "127.0.0.1",
        ),
    ]

    raw_df = spark.createDataFrame(duplicate_rows, schema=TRANSACTION_SCHEMA)
    clean_df, _ = validate_and_route_records(raw_df)
    silver_df = deduplicate_and_enrich_silver(clean_df)

    # Duplicate pruned
    assert silver_df.count() == 1

    winner = silver_df.collect()[0]
    assert winner["transaction_id"] == "tx_dup_new"
    # Calendar partitions extracted
    assert winner["year"] == 2026
    assert winner["month"] == "09"
    assert winner["day"] == "13"