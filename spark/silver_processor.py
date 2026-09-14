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
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] SilverProcessor: %(message)s",
)
logger = logging.getLogger("SilverProcessor")

bronze_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("idempotency_key", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source_ip", StringType(), True)
])


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

    # Pack small JSON files into larger partitions to eliminate HTTP request overhead
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
    spark.conf.set("spark.sql.files.openCostInBytes", "4194304")       # 4 MB
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    try:
        logger.info(f"Scanning raw files from Bronze: {bronze_s3_path}")
        raw_df = (
            spark.read.schema(bronze_schema)
            .option("mode", "PERMISSIVE")
            .json(bronze_s3_path)
        )

        clean_df, _ = validate_and_route_records(raw_df)

        # Deduplicate and extract partition keys
        silver_df = deduplicate_and_enrich_silver(clean_df)

        logger.info(
            f"Writing Parquet files partitioned by date to: {silver_s3_path}"
        )
        
        # Coalesce to 4 files before writing to minimize S3 multipart upload overhead
        (
            silver_df.coalesce(4)
            .write.mode("overwrite")
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