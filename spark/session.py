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

    logger.info(f"Configuring PySpark session: {app_name}")

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/app/spark/log4j2.properties")
        
        # Committer & S3A fast upload optimizations
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        
        # S3A FileSystem Implementation & endpoints
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        
        # Timeout & connection tuning
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.session.token.expiration", "86400")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        
        # Optimize Small JSON File Reads from S3
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB batching
        .config("spark.sql.files.openCostInBytes", "4194304")       # 4 MB
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

    # getOrCreate() called ONCE at the end
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("PySpark session successfully established with S3A support.")
    return spark