USE SCHEMA EKLAKSHAY_DW.GOLD;

-- 1. Row Count & Volume Reconciliation (Silver vs. Fact)
SELECT 
    'Row Count Match' AS test_name,
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) AS silver_count,
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS) AS gold_count,
    CASE 
        WHEN (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) = 
             (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS) 
        THEN 'PASS' ELSE 'FAIL' 
    END AS status
UNION ALL
SELECT 
    'Settled Amount Match' AS test_name,
    (SELECT ROUND(SUM(amount), 2) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS WHERE status = 'SUCCESS'),
    (SELECT ROUND(SUM(amount), 2) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS WHERE status = 'SUCCESS'),
    CASE 
        WHEN (SELECT ROUND(SUM(amount), 2) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS WHERE status = 'SUCCESS') = 
             (SELECT ROUND(SUM(amount), 2) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS WHERE status = 'SUCCESS') 
        THEN 'PASS' ELSE 'FAIL' 
    END AS status;

-- 2. Foreign Key & Orphan Check
SELECT 
    'Orphaned User Keys' AS check_name,
    COUNT(*) AS orphan_count
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u ON f.user_key = u.user_key
WHERE u.user_key IS NULL
UNION ALL
SELECT 
    'Orphaned Merchant Keys' AS check_name,
    COUNT(*) AS orphan_count
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m ON f.merchant_key = m.merchant_key
WHERE m.merchant_key IS NULL
UNION ALL
SELECT 
    'Orphaned Date Keys' AS check_name,
    COUNT(*) AS orphan_count
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;

-- 3. Business Rule Validation: Negative Amounts & Duplicate Idempotency Keys
SELECT 
    'Negative or Zero Amounts' AS test_name,
    COUNT(*) AS violation_count
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS
WHERE amount <= 0
UNION ALL
SELECT 
    'Duplicate Idempotency Keys' AS test_name,
    COUNT(*) - COUNT(DISTINCT idempotency_key) AS violation_count
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS;



SELECT 
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) AS silver_count,
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS) AS gold_fact_count;

SELECT 
    COUNT(*) AS total_silver,
    COUNT(c.customer_key) AS matched_customers,
    COUNT(p.product_key) AS matched_products
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS c 
    ON s.customer_id = c.customer_id
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_PRODUCTS p 
    ON s.product_id = p.product_id;




USE DATABASE EKLAKSHAY_DW;
USE SCHEMA GOLD;

-- 1. Default record for unknown users
MERGE INTO DIM_USERS target
USING (
    SELECT -1 AS USER_KEY, 'UNKNOWN' AS USER_ID, '1970-01-01'::TIMESTAMP AS FIRST_SEEN_AT, '1970-01-01'::TIMESTAMP AS LAST_SEEN_AT
) source
ON target.USER_KEY = source.USER_KEY
WHEN NOT MATCHED THEN 
  INSERT (USER_KEY, USER_ID, FIRST_SEEN_AT, LAST_SEEN_AT) 
  VALUES (source.USER_KEY, source.USER_ID, source.FIRST_SEEN_AT, source.LAST_SEEN_AT);

-- 2. Default record for unknown merchants
MERGE INTO DIM_MERCHANTS target
USING (
    SELECT -1 AS MERCHANT_KEY, 'UNKNOWN' AS MERCHANT_ID, '1970-01-01'::TIMESTAMP AS FIRST_TRANSACTION_AT, '1970-01-01'::TIMESTAMP AS LAST_TRANSACTION_AT
) source
ON target.MERCHANT_KEY = source.MERCHANT_KEY
WHEN NOT MATCHED THEN 
  INSERT (MERCHANT_KEY, MERCHANT_ID, FIRST_TRANSACTION_AT, LAST_TRANSACTION_AT) 
  VALUES (source.MERCHANT_KEY, source.MERCHANT_ID, source.FIRST_TRANSACTION_AT, source.LAST_TRANSACTION_AT);

-- 3. Default record for unknown/missing dates
MERGE INTO DIM_DATE target
USING (
    SELECT 
        -1 AS DATE_KEY, 
        '1970-01-01'::DATE AS FULL_DATE, 
        1970 AS YEAR, 
        1 AS MONTH, 
        'January' AS MONTH_NAME, 
        1 AS DAY_OF_MONTH, 
        4 AS DAY_OF_WEEK, 
        FALSE AS IS_WEEKEND
) source
ON target.DATE_KEY = source.DATE_KEY
WHEN NOT MATCHED THEN 
  INSERT (DATE_KEY, FULL_DATE, YEAR, MONTH, MONTH_NAME, DAY_OF_MONTH, DAY_OF_WEEK, IS_WEEKEND) 
  VALUES (source.DATE_KEY, source.FULL_DATE, source.YEAR, source.MONTH, source.MONTH_NAME, source.DAY_OF_MONTH, source.DAY_OF_WEEK, source.IS_WEEKEND);




DESC TABLE EKLAKSHAY_DW.GOLD.DIM_USERS;
DESC TABLE EKLAKSHAY_DW.GOLD.DIM_MERCHANTS;
DESC TABLE EKLAKSHAY_DW.GOLD.DIM_DATE;

TRUNCATE TABLE EKLAKSHAY_DW.SILVER.TRANSACTIONS;
TRUNCATE TABLE EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS;


INSERT OVERWRITE INTO EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS
SELECT
    s.transaction_id,
    COALESCE(u.user_key, -1) AS user_key,
    COALESCE(m.merchant_key, -1) AS merchant_key,
    COALESCE(d.date_key, -1) AS date_key,
    s.amount,
    s.currency,
    s.status,
    s.event_timestamp
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u
    ON s.user_id = u.user_id
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m
    ON s.merchant_id = m.merchant_id
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d
    ON TO_NUMBER(TO_VARCHAR(s.event_timestamp, 'YYYYMMDD')) = d.date_key;

DESC TABLE EKLAKSHAY_DW.SILVER.TRANSACTIONS;
DESC TABLE EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS;

INSERT OVERWRITE INTO EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS (
    TRANSACTION_ID,
    IDEMPOTENCY_KEY,
    USER_KEY,
    MERCHANT_KEY,
    DATE_KEY,
    AMOUNT,
    CURRENCY,
    PAYMENT_METHOD,
    STATUS,
    EVENT_TIMESTAMP,
    IP_ADDRESS,
    CREATED_AT
)
SELECT
    s.transaction_id,
    s.idempotency_key,
    COALESCE(u.user_key, -1) AS user_key,
    COALESCE(m.merchant_key, -1) AS merchant_key,
    COALESCE(d.date_key, -1) AS date_key,
    s.amount,
    s.currency,
    s.payment_method,
    s.status,
    s.event_timestamp,
    s.ip_address,
    CURRENT_TIMESTAMP() AS created_at
FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS s
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_USERS u
    ON s.user_id = u.user_id
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m
    ON s.merchant_id = m.merchant_id
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d
    ON TO_NUMBER(TO_VARCHAR(s.event_timestamp, 'YYYYMMDD')) = d.date_key;


SELECT 
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) AS silver_count,
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS) AS gold_count;






--phase 6::............................
--financial summary table
CREATE OR REPLACE VIEW EKLAKSHAY_DW.GOLD.V_DAILY_FINANCIAL_SUMMARY AS
SELECT
    d.full_date,
    d.year,
    d.month_name,
    COUNT(f.transaction_id) AS total_transactions,
    COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS successful_transactions,
    COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) AS failed_transactions,
    ROUND(SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END), 2) AS gross_merchandise_value,
    ROUND(AVG(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE NULL END), 2) AS avg_ticket_size,
    ROUND(
        COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(f.transaction_id), 0),
        2
    ) AS payment_success_rate
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_DATE d
    ON f.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month_name
ORDER BY d.full_date DESC;

SELECT * FROM EKLAKSHAY_DW.GOLD.V_DAILY_FINANCIAL_SUMMARY;

--merchant performance
CREATE OR REPLACE VIEW EKLAKSHAY_DW.GOLD.V_MERCHANT_PERFORMANCE AS
SELECT
    COALESCE(m.merchant_id, 'UNKNOWN') AS merchant_id,
    m.first_transaction_at,
    m.last_transaction_at,
    COUNT(f.transaction_id) AS total_transactions,
    ROUND(SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END), 2) AS total_gross_volume,
    COUNT(DISTINCT f.user_key) AS unique_customers,
    ROUND(
        COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) * 100.0 / NULLIF(COUNT(f.transaction_id), 0),
        2
    ) AS failure_rate_pct
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
LEFT JOIN EKLAKSHAY_DW.GOLD.DIM_MERCHANTS m
    ON f.merchant_key = m.merchant_key
GROUP BY 1, 2, 3
ORDER BY total_gross_volume DESC;

SELECT * FROM EKLAKSHAY_DW.GOLD.V_MERCHANT_PERFORMANCE LIMIT 5;


-- PAYMENT CHANNEL AND COHORT MATRICS
CREATE OR REPLACE VIEW EKLAKSHAY_DW.GOLD.V_PAYMENT_CHANNEL_METRICS AS
SELECT
    COALESCE(f.payment_method, 'UNKNOWN') AS payment_method,
    f.currency,
    COUNT(f.transaction_id) AS total_attempts,
    COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS successful_count,
    COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) AS failed_count,
    ROUND(SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END), 2) AS total_settled_volume,
    ROUND(
        COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) * 100.0 / NULLIF(COUNT(f.transaction_id), 0),
        2
    ) AS conversion_rate_pct
FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS f
GROUP BY 1, 2
ORDER BY total_settled_volume DESC;

SELECT * FROM EKLAKSHAY_DW.GOLD.V_PAYMENT_CHANNEL_METRICS;


--just for testing airflow dag
SELECT 
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.SILVER.TRANSACTIONS) AS silver_count,
    (SELECT COUNT(*) FROM EKLAKSHAY_DW.GOLD.FACT_TRANSACTIONS) AS gold_count,
    (SELECT SUM(total_transactions) FROM EKLAKSHAY_DW.GOLD.V_DAILY_FINANCIAL_SUMMARY) AS view_summary_count;
