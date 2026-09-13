import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] SparkSessionBuilder: %(message)s",
)
logger = logging.getLogger("SparkSessionBuilder")


def get_spark_session(app_name: str = "EkLakshay-Silver-Transformer") -> SparkSession:
    """Initializes a local PySpark session with clean S3A parameters."""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

    HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
    AWS_SDK_PACKAGE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

    logger.info(f"Configuring PySpark session: {app_name}")

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/app/spark/log4j2.properties")
        # Attach AWS S3A dependencies
        .config(
            "spark.jars.packages",
            f"{HADOOP_AWS_PACKAGE},{AWS_SDK_PACKAGE}",
        )
        # S3A FileSystem Implementation
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Override ALL duration string properties to pure integer values (seconds / ms)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.session.token.expiration", "86400")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    )

    if aws_access_key and aws_secret_key:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("PySpark session successfully established with S3A support.")
    return spark