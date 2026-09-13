from spark.session import get_spark_session


def test_spark_session_instantiation():
    spark = get_spark_session(app_name="EkLakshay-Session-Test")
    assert spark is not None
    assert spark.version is not None
    
    # Test a simple DataFrame operation in memory
    df = spark.createDataFrame([(1, "valid"), (2, "test")], ["id", "status"])
    assert df.count() == 2