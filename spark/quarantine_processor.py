import os
import logging
from dotenv import load_dotenv
from pyspark.sql import functions as F
from spark.session import get_spark_session
from spark.transformations import validate_and_route_records, TRANSACTION_SCHEMA

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] QuarantineProcessor: %(message)s",
)
logger = logging.getLogger("QuarantineProcessor")


def process_bronze_to_quarantine():
    """Reads raw NDJSON from S3 Bronze, isolates bad records,

    and writes them to S3 Quarantine with diagnostic tags.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME is not set in environment.")

    bronze_s3_path = f"s3a://{bucket_name}/bronze/*/*/*/*/*.json"
    quarantine_s3_path = f"s3a://{bucket_name}/quarantine/"

    logger.info("Initializing Spark session for Bronze-to-Quarantine processing...")
    spark = get_spark_session(app_name="EkLakshay-Bronze-Quarantine-Router")

    logger.info(f"Reading raw NDJSON files from: {bronze_s3_path}")
    # Read raw files using our explicit schema
    raw_df = (
        spark.read.schema(TRANSACTION_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(bronze_s3_path)
    )

    total_raw_count = raw_df.count()
    logger.info(f"Total raw transactions scanned from Bronze: {total_raw_count}")

    if total_raw_count == 0:
        logger.warning("No records found in Bronze partition. Exiting job.")
        spark.stop()
        return

    # Route records into clean and quarantine DataFrames
    clean_df, quarantine_df = validate_and_route_records(raw_df)

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    logger.info(
        f"Validation Results -> Clean: {clean_count} records | "
        f"Quarantine (DLQ): {quarantine_count} records"
    )

    if quarantine_count > 0:
        logger.info(f"Writing {quarantine_count} quarantined records to: {quarantine_s3_path}")
        (
            quarantine_df.write.mode("append")
            .partitionBy("quarantine_reason")
            .json(quarantine_s3_path)
        )
        logger.info("Successfully committed bad records to S3 Quarantine.")
    else:
        logger.info("Zero records violated contract. Quarantine write skipped.")

    spark.stop()
    logger.info("Job completed successfully.")


if __name__ == "__main__":
    process_bronze_to_quarantine()