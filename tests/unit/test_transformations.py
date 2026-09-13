from spark.session import get_spark_session
from spark.transformations import validate_and_route_records, TRANSACTION_SCHEMA


def test_validate_and_route_records():
    spark = get_spark_session(app_name="EkLakshay-Transform-Test")

    # Sample batch containing 1 valid row, 1 negative amount, and 1 bad currency
    sample_data = [
        (
            "tx_001",
            "idemp_01",
            "usr_1",
            "merch_1",
            500.0,
            "INR",
            "UPI",
            "SUCCESS",
            "2026-09-13T10:00:00",
            "192.168.1.1",
        ),
        (
            "tx_002",
            "idemp_02",
            "usr_2",
            "merch_2",
            -150.0,
            "USD",
            "CREDIT_CARD",
            "SUCCESS",
            "2026-09-13T10:05:00",
            "192.168.1.2",
        ),
        (
            "tx_003",
            "idemp_03",
            "usr_3",
            "merch_3",
            250.0,
            "CRYPTO_ETH",
            "UPI",
            "SUCCESS",
            "2026-09-13T10:10:00",
            "192.168.1.3",
        ),
    ]

    raw_df = spark.createDataFrame(sample_data, schema=TRANSACTION_SCHEMA)
    clean_df, quarantine_df = validate_and_route_records(raw_df)

    # 1. Clean checks
    assert clean_df.count() == 1
    assert clean_df.collect()[0]["transaction_id"] == "tx_001"

    # 2. Quarantine checks
    assert quarantine_df.count() == 2
    quarantined_reasons = [row["quarantine_reason"] for row in quarantine_df.collect()]
    assert "INVALID_OR_NEGATIVE_AMOUNT" in quarantined_reasons
    assert "UNSUPPORTED_CURRENCY" in quarantined_reasons