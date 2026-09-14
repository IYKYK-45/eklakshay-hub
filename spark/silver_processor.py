import logging
import os
from dotenv import load_dotenv
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from spark.session import get_spark_session
from spark.transformations import (
    TRANSACTION_SCHEMA,
    deduplicate_and_enrich_silver,
    validate_and_route_records,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] SilverProcessor: %(message)s",
)
logger = logging.getLogger("SilverProcessor")


def process_bronze_to_silver():
    """Ingests raw Bronze data, isolates clean events, deduplicates by idempotency key,

    and writes optimized Snappy Parquet to the Silver S3 bucket.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME is not set in environment.")

    bronze_s3_path = f"s3a://{bucket_name}/bronze/*/*/*/*/*.json"
    silver_s3_path = f"s3a://{bucket_name}/silver/"

    logger.info("Starting Bronze-to-Silver Spark Pipeline...")
    spark = get_spark_session(app_name="EkLakshay-Silver-Processor")

    try:
        logger.info(f"Scanning raw files from Bronze: {bronze_s3_path}")
        raw_df = (
            spark.read.schema(TRANSACTION_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json(bronze_s3_path)
        )

        clean_df, _ = validate_and_route_records(raw_df)
        clean_count = clean_df.count()
        logger.info(f"Clean records identified: {clean_count}")

        if clean_count == 0:
            logger.warning("No valid clean records to process. Exiting.")
            return

        # Deduplicate and extract partition keys
        silver_df = deduplicate_and_enrich_silver(clean_df)
        silver_count = silver_df.count()
        duplicates_removed = clean_count - silver_count

        logger.info(
            f"Deduplication complete -> Retained: {silver_count} | Duplicates pruned: {duplicates_removed}"
        )

        logger.info(
            f"Writing Parquet files partitioned by date to: {silver_s3_path}"
        )
        (
            silver_df.write.mode("append")
            .partitionBy("year", "month", "day")
            .option("compression", "snappy")
            .parquet(silver_s3_path)
        )

        logger.info(
            "Successfully persisted clean dataset into Silver Parquet layer."
        )

    except (AnalysisException, Py4JJavaError) as e:
        if "Path does not exist" in str(e):
            logger.warning(
                f"No Bronze source files found at {bronze_s3_path}. Skipping."
            )
        else:
            raise e
    finally:
        logger.info("Shutting down Spark session.")
        spark.stop()


if __name__ == "__main__":
    process_bronze_to_silver()