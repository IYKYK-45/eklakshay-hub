from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.docker.operators.docker import DockerOperator
import os


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_transactions_pipeline",
    default_args=default_args,
    description="End-to-end Spark ETL to Snowflake Star Schema",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["spark", "s3", "snowflake", "gold"],
) as dag:

    # 1. Run Spark Silver Transformation Job

    run_spark_silver = BashOperator(
    task_id="run_spark_silver_processing",
    bash_command="docker exec eklakshay-spark python /app/spark/silver_processor.py",
)
    
    # 2. Ingest S3 Silver Parquet into Snowflake Silver Staging Table
    copy_to_silver = SnowflakeOperator(
        task_id="copy_s3_to_snowflake_silver",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO EKLAKSHAY_DW.SILVER.TRANSACTIONS (
                transaction_id,
                idempotency_key,
                user_id,
                merchant_id,
                amount,
                currency,
                payment_method,
                status,
                event_timestamp,
                ip_address,
                year,
                month,
                day
            )
            FROM (
                SELECT 
                    $1:transaction_id::STRING,
                    $1:idempotency_key::STRING,
                    $1:user_id::STRING,
                    $1:merchant_id::STRING,
                    $1:amount::NUMBER(12, 2),
                    $1:currency::STRING,
                    $1:payment_method::STRING,
                    $1:status::STRING,
                    $1:event_timestamp::TIMESTAMP_NTZ,
                    $1:source_ip::STRING,
                    REGEXP_SUBSTR(METADATA$FILENAME, 'year=([0-9]{4})', 1, 1, 'e', 1)::INT,
                    REGEXP_SUBSTR(METADATA$FILENAME, 'month=([0-9]{2})', 1, 1, 'e', 1)::STRING,
                    REGEXP_SUBSTR(METADATA$FILENAME, 'day=([0-9]{2})', 1, 1, 'e', 1)::STRING
                FROM @EKLAKSHAY_DW.SILVER.s3_silver_stage_direct
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*\\.parquet';
        """,
    )

    # 3. Incrementally Populate Gold Dimensions
    update_gold_dimensions = SnowflakeOperator(
        task_id="update_gold_dimensions",
        snowflake_conn_id="snowflake_default",
        sql="""
            -- Update Users Dimension
            INSERT INTO EKLAKSHAY_DW.GOLD.DIM_USERS (user_id, first_seen_at, last_seen_at)
            SELECT 
                s.user_id,
                MIN(s.event_timestamp),
                MAX(s.event_timestamp)
            FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
            LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u ON s.user_id = u.user_id
            WHERE u.user_id IS NULL
            GROUP BY s.user_id;

            -- Update Merchants Dimension
            INSERT INTO EKLAKSHAY_DW.GOLD.DIM_MERCHANTS (merchant_id, first_transaction_at, last_transaction_at)
            SELECT 
                s.merchant_id,
                MIN(s.event_timestamp),
                MAX(s.event_timestamp)
            FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
            LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m ON s.merchant_id = m.merchant_id
            WHERE m.merchant_id IS NULL
            GROUP BY s.merchant_id;
        """,
    )

    # 4. Insert into Fact Table (Idempotent Append)
    # 4. Insert into Fact Table (Idempotent Overwrite)
    load_gold_fact = SnowflakeOperator(
        task_id="load_gold_fact_table",
        snowflake_conn_id="snowflake_default",
        sql="""
        INSERT OVERWRITE INTO EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS (
            transaction_id,
            idempotency_key,
            user_key,
            merchant_key,
            date_key,
            amount,
            currency,
            payment_method,
            status,
            event_timestamp,
            ip_address
        )
        SELECT
            s.transaction_id,
            s.idempotency_key,
            COALESCE(u.user_key, -1) AS user_key,
            COALESCE(m.merchant_key, -1) AS merchant_key,
            TO_NUMBER(TO_CHAR(s.event_timestamp::DATE, 'YYYYMMDD')) AS date_key,
            s.amount,
            s.currency,
            s.payment_method,
            s.status,
            s.event_timestamp,
            s.ip_address
        FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
        LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u 
            ON s.user_id = u.user_id
        LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m 
            ON s.merchant_id = m.merchant_id;
        """,
    )

    # 5. Quality Gate: Assert Fact and Silver Row Balance
    # 5. Quality Gate: Assert Fact and Silver Row Balance
    run_quality_gate = SnowflakeOperator(
        task_id="assert_data_reconciliation",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT 
                CASE 
                    WHEN (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) = 
                         (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS)
                    THEN 1
                    ELSE 1 / 0
                END;
        """,
    )

    # Define Dependency Flow
    run_spark_silver >> copy_to_silver >> update_gold_dimensions >> load_gold_fact >> run_quality_gate