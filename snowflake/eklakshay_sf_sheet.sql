-- 1. Use ACCOUNTADMIN role to create integrations
USE ROLE ACCOUNTADMIN;

-- 1. Create and select a dedicated database for the platform
CREATE DATABASE IF NOT EXISTS EKLAKSHAY_DW;
USE DATABASE EKLAKSHAY_DW;

-- 2. Create and select a staging schema for ingested lakehouse data
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- 2. Create the Storage Integration pointing to your Silver prefix
CREATE OR REPLACE STORAGE INTEGRATION s3_silver_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::151844461258:role/SnowflakeS3SilverRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://eklakshay-data-lake-garvit-2102/silver/');

-- 3. Describe the integration to extract Snowflake's IAM identity
DESC INTEGRATION s3_silver_integration;

-- 3. Now create the file format inside EKLAKSHAY_DW.SILVER
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;

-- Refresh the integration
ALTER STORAGE INTEGRATION s3_silver_integration SET ENABLED = TRUE;

-- Re-create the stage to invalidate internal cache
CREATE OR REPLACE STAGE s3_silver_stage
  STORAGE_INTEGRATION = s3_silver_integration
  URL = 's3://eklakshay-data-lake-garvit-2102/silver/'
  FILE_FORMAT = (TYPE = PARQUET);

LIST @s3_silver_stage;


-- Query the Staged Parquet Files Directly
SELECT 
    $1:transaction_id::STRING AS transaction_id,
    $1:idempotency_key::STRING AS idempotency_key,
    $1:user_id::STRING AS user_id,
    $1:merchant_id::STRING AS merchant_id,
    $1:amount::NUMBER(12, 2) AS amount,
    $1:currency::STRING AS currency,
    $1:payment_method::STRING AS payment_method,
    $1:status::STRING AS status,
    $1:event_timestamp::TIMESTAMP_NTZ AS event_timestamp,
    $1:ip_address::STRING AS ip_address,
    $1:year::INT AS year,
    $1:month::STRING AS month,
    $1:day::STRING AS day
FROM @s3_silver_stage_direct
(FILE_FORMAT => 'parquet_format', PATTERN => '.*[.]parquet')
LIMIT 10;


-- Create the Typed Silver Staging Table
CREATE TABLE IF NOT EXISTS EKLAKSHAY_DW.SILVER.TRANSACTIONS (
    transaction_id STRING NOT NULL,
    idempotency_key STRING NOT NULL,
    user_id STRING NOT NULL,
    merchant_id STRING NOT NULL,
    amount NUMBER(12, 2) NOT NULL,
    currency STRING NOT NULL,
    payment_method STRING NOT NULL,
    status STRING NOT NULL,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    ip_address STRING,
    year INT,
    month STRING,
    day STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (transaction_id)
);

SELECT 
    OBJECT_KEYS($1) AS column_keys,
    $1 AS raw_record
FROM @s3_silver_stage_direct
(FILE_FORMAT => 'parquet_format', PATTERN => '.*[.]parquet')
LIMIT 1;

-- load data using copy into
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
    FROM @s3_silver_stage_direct
)
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*\\.parquet';

SELECT COUNT(*) AS total_loaded_records FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS;
SELECT * FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS LIMIT 5;


-- let's create gold layer now which will have tables of analytical data for futhur processing and entrprise sharing
CREATE SCHEMA IF NOT EXISTS EKLAKSHAY_DW.GOLD;
USE SCHEMA EKLAKSHAY_DW.GOLD;

-- 1. Date Dimension (Pre-populates an analytical date spine)
CREATE TABLE IF NOT EXISTS EKLAKSHAY_DW.GOLD.DIM_DATE (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name STRING NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week STRING NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

INSERT INTO EKLAKSHAY_DW.GOLD.DIM_DATE
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730)) -- 2 years of dates
)
SELECT 
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    DAY(full_date) AS day_of_month,
    DAYNAME(full_date) AS day_of_week,
    CASE WHEN DAYNAME(full_date) IN ('Sat', 'Sun') THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine;

select * from EKLAKSHAY_DW.GOLD.DIM_DATE;

-- 2. User Dimension (Derived from Silver)
CREATE TABLE IF NOT EXISTS EKLAKSHAY_DW.GOLD.DIM_USERS (
    user_key INT AUTOINCREMENT PRIMARY KEY,
    user_id STRING NOT NULL UNIQUE,
    first_seen_at TIMESTAMP_NTZ,
    last_seen_at TIMESTAMP_NTZ
);

INSERT INTO EKLAKSHAY_DW.GOLD.DIM_USERS (user_id, first_seen_at, last_seen_at)
SELECT 
    user_id,
    MIN(event_timestamp) AS first_seen_at,
    MAX(event_timestamp) AS last_seen_at
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS
GROUP BY user_id;

select * from EKLAKSHAY_DW.GOLD.DIM_USERS;

-- 3. Merchant Dimension (Derived from Silver)
CREATE TABLE IF NOT EXISTS EKLAKSHAY_DW.GOLD.DIM_MERCHANTS (
    merchant_key INT AUTOINCREMENT PRIMARY KEY,
    merchant_id STRING NOT NULL UNIQUE,
    first_transaction_at TIMESTAMP_NTZ,
    last_transaction_at TIMESTAMP_NTZ
);

INSERT INTO EKLAKSHAY_DW.GOLD.DIM_MERCHANTS (merchant_id, first_transaction_at, last_transaction_at)
SELECT 
    merchant_id,
    MIN(event_timestamp) AS first_transaction_at,
    MAX(event_timestamp) AS last_transaction_at
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS
GROUP BY merchant_id;

SELECT * FROM EKLAKSHAY_DW.GOLD.DIM_MERCHANTS


-- CENTRAL FACT TABLE
CREATE TABLE IF NOT EXISTS EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS (
    fact_transaction_key INT AUTOINCREMENT PRIMARY KEY,
    transaction_id STRING NOT NULL UNIQUE,
    idempotency_key STRING NOT NULL,
    user_key INT FOREIGN KEY REFERENCES EKLAKSHAY_DW.GOLD.DIM_USERS(user_key),
    merchant_key INT FOREIGN KEY REFERENCES EKLAKSHAY_DW.GOLD.DIM_MERCHANTS(merchant_key),
    date_key INT FOREIGN KEY REFERENCES EKLAKSHAY_DW.GOLD.DIM_DATE(date_key),
    amount NUMBER(12, 2) NOT NULL,
    currency STRING NOT NULL,
    payment_method STRING NOT NULL,
    status STRING NOT NULL,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    ip_address STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate Fact Table from Silver joined with Dimensions
INSERT INTO EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS (
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
    u.user_key,
    m.merchant_key,
    TO_NUMBER(TO_CHAR(s.event_timestamp::DATE, 'YYYYMMDD')) AS date_key,
    s.amount,
    s.currency,
    s.payment_method,
    s.status,
    s.event_timestamp,
    s.ip_address
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u 
  ON s.user_id = u.user_id
JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m 
  ON s.merchant_id = m.merchant_id;

SELECT * FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS LIMIT 10;


-- SAMPLE TESTING 
SELECT 
    d.month_name,
    f.payment_method,
    f.status,
    COUNT(f.fact_transaction_key) AS total_transactions,
    SUM(f.amount) AS total_volume_usd,
    AVG(f.amount) AS avg_ticket_size
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d 
  ON f.date_key = d.date_key
GROUP BY d.month_name, f.payment_method, f.status
ORDER BY total_volume_usd DESC;


-- creating views for Analytical Marts & Reporting Views
USE SCHEMA EKLAKSHAY_DW.GOLD;

-- 1. Daily Financial Performance Mart
CREATE OR REPLACE VIEW V_DAILY_FINANCIAL_SUMMARY AS
SELECT 
    d.full_date,
    d.day_of_week,
    d.is_weekend,
    f.currency,
    f.payment_method,
    COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS successful_txns,
    COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) AS failed_txns,
    ROUND(COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS failure_rate_pct,
    ROUND(SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END), 2) AS gross_settled_volume,
    ROUND(AVG(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE NULL END), 2) AS avg_transaction_value
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_of_week, d.is_weekend, f.currency, f.payment_method;

-- 2. Merchant Risk & Volume Leaderboard
CREATE OR REPLACE VIEW V_MERCHANT_PERFORMANCE AS
SELECT 
    m.merchant_id,
    COUNT(f.fact_transaction_key) AS total_attempted_txns,
    COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS successful_txns,
    ROUND(SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END), 2) AS total_revenue,
    COUNT(DISTINCT f.user_key) AS unique_customers,
    COUNT(DISTINCT f.ip_address) AS distinct_ips_used,
    ROUND(COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS merchant_failure_rate_pct
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m ON f.merchant_key = m.merchant_key
GROUP BY m.merchant_id;


SELECT * FROM V_DAILY_FINANCIAL_SUMMARY ORDER BY full_date DESC LIMIT 10;
SELECT * FROM V_MERCHANT_PERFORMANCE ORDER BY total_revenue DESC LIMIT 10;