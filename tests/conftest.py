import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provides a lightweight, in-memory Spark session for fast local unit testing."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("EkLakshay-UnitTest-Suite")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()