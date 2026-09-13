from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Strict PySpark contract mirroring Pydantic schema
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("idempotency_key", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_ip", StringType(), True),
    ]
)

VALID_CURRENCIES = ["INR", "USD", "EUR", "GBP"]
VALID_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING"]
VALID_STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def validate_and_route_records(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Splits raw transactions into a valid dataset and a quarantine (DLQ) dataset.

    Attaches failure reason tags to quarantined records for auditability.
    """
    # Define boolean condition flags for data hygiene
    has_valid_id = F.col("transaction_id").isNotNull() & (
        F.length(F.col("transaction_id")) > 0
    )
    has_positive_amount = F.col("amount").isNotNull() & (F.col("amount") > 0.0)
    has_valid_currency = F.col("currency").isin(VALID_CURRENCIES)
    has_valid_method = F.col("payment_method").isin(VALID_METHODS)
    has_valid_status = F.col("status").isin(VALID_STATUSES)

    # Master validity condition
    is_valid_record = (
        has_valid_id
        & has_positive_amount
        & has_valid_currency
        & has_valid_method
        & has_valid_status
    )

    # Clean DataFrame: Keep valid records and parse event_timestamp to real TimestampType
    clean_df = (
        df.filter(is_valid_record)
        .withColumn(
            "event_timestamp", F.to_timestamp("event_timestamp")
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    # Quarantine DataFrame: Keep failed records and tag the exact root cause
    quarantine_df = df.filter(~is_valid_record).withColumn(
        "quarantine_reason",
        F.when(~has_valid_id, "MISSING_OR_EMPTY_TRANSACTION_ID")
        .when(~has_positive_amount, "INVALID_OR_NEGATIVE_AMOUNT")
        .when(~has_valid_currency, "UNSUPPORTED_CURRENCY")
        .when(~has_valid_method, "UNSUPPORTED_PAYMENT_METHOD")
        .when(~has_valid_status, "UNRECOGNIZED_STATUS")
        .otherwise("UNKNOWN_VALIDATION_ERROR"),
    ).withColumn("quarantined_at", F.current_timestamp())

    return clean_df, quarantine_df