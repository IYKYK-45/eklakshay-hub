from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

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
    """Splits raw transactions into clean and quarantine datasets.

    Safely handles malformed timestamps without crashing under ANSI mode.
    """
    # 1. Safely attempt to parse the timestamp (returns NULL on invalid strings)
    # try_to_timestamp prevents runtime ANSI CAST_INVALID_INPUT exceptions
    parsed_df = df.withColumn(
        "parsed_timestamp",
        F.expr("try_to_timestamp(event_timestamp)"),
    )

    # 2. Validation criteria
    has_valid_id = F.col("transaction_id").isNotNull() & (
        F.length(F.col("transaction_id")) > 0
    )
    has_positive_amount = F.col("amount").isNotNull() & (F.col("amount") > 0.0)
    has_valid_currency = F.col("currency").isin(VALID_CURRENCIES)
    has_valid_method = F.col("payment_method").isin(VALID_METHODS)
    has_valid_status = F.col("status").isin(VALID_STATUSES)
    has_valid_timestamp = F.col("parsed_timestamp").isNotNull()

    # Master record validity
    is_valid_record = (
        has_valid_id
        & has_positive_amount
        & has_valid_currency
        & has_valid_method
        & has_valid_status
        & has_valid_timestamp
    )

    # 3. Clean Stream: replace string timestamp with parsed timestamp
    clean_df = (
        parsed_df.filter(is_valid_record)
        .withColumn("event_timestamp", F.col("parsed_timestamp"))
        .drop("parsed_timestamp")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    # 4. Quarantine Stream: tag root causes including timestamp errors
    quarantine_df = (
        parsed_df.filter(~is_valid_record)
        .withColumn(
            "quarantine_reason",
            F.when(~has_valid_id, "MISSING_OR_EMPTY_TRANSACTION_ID")
            .when(~has_positive_amount, "INVALID_OR_NEGATIVE_AMOUNT")
            .when(~has_valid_currency, "UNSUPPORTED_CURRENCY")
            .when(~has_valid_method, "UNSUPPORTED_PAYMENT_METHOD")
            .when(~has_valid_status, "UNRECOGNIZED_STATUS")
            .when(~has_valid_timestamp, "INVALID_TIMESTAMP_FORMAT")
            .otherwise("UNKNOWN_VALIDATION_ERROR"),
        )
        .drop("parsed_timestamp")
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return clean_df, quarantine_df


def deduplicate_and_enrich_silver(clean_df: DataFrame) -> DataFrame:
    """Deduplicates records based on idempotency_key and derives calendar partition keys."""
    window_spec = Window.partitionBy("idempotency_key").orderBy(
        F.col("event_timestamp").desc(),
        F.col("ingestion_timestamp").desc(),
    )

    deduped_df = (
        clean_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    enriched_df = (
        deduped_df.withColumn("year", F.year(F.col("event_timestamp")))
        .withColumn("month", F.format_string("%02d", F.month(F.col("event_timestamp"))))
        .withColumn("day", F.format_string("%02d", F.dayofmonth(F.col("event_timestamp"))))
    )

    return enriched_df