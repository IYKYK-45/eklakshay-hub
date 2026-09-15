# Cloud E-Commerce Lakehouse & Analytics Platform
> **Scalable, idempotent batch data pipeline processing 187k+ transactions from AWS S3 to Snowflake via PySpark and Apache Airflow, served via Power BI.**

---

## 🏗️ Architecture Overview

The platform implements a **Medallion (Bronze-Silver-Gold) Lakehouse Architecture**:


+------------------+      +-------------------+      +----------------------+      +--------------------+
|  AWS S3 (Bronze) | ---> |  PySpark (Silver) | ---> | Snowflake DW (Gold)  | ---> |  Power BI Desktop  |
|  Raw Event JSONs |      | Cleaned Parquet   |      | Star Schema + Views  |      | Executive Reporting|
+------------------+      +-------------------+      +----------------------+      +--------------------+
^                           ^
|                           |
+-----------------------------------------------+
|           Apache Airflow Orchestrator         |
|       (Dockerized Workflow Management)        |
+-----------------------------------------------+


1. **Bronze Layer (Raw):** Event-driven transaction JSON payloads partitioned and landed in AWS S3.
2. **Silver Layer (Cleansed):** Distributed batch processing using PySpark to enforce schema compliance, cast timestamps, filter corrupt records, and persist to partitioned snappy-compressed Parquet.
3. **Gold Layer (Curated DW):** Kimball Star Schema implemented in Snowflake featuring surrogate key mappings, fallback handling (`-1`), and pre-aggregated analytical serving views.
4. **BI & Serving:** Interactive executive dashboard connected to Snowflake Gold views for transaction monitoring, merchant ranking, and conversion analysis.

---

## 🚀 Key Engineering Achievements & Optimizations

### 1. PySpark Distributed Performance Tuning (68% Runtime Reduction)
* **Problem:** Initial Spark execution required **9.5 minutes** due to small-file overhead (thousands of micro-JSONs) and redundant actions generating 3,900+ stages.
* **Solution:**
  * Eliminated redundant dataframe `.count()` actions triggering repeated S3 scans.
  * Implemented file packing using `spark.sql.files.maxPartitionBytes` (128MB) and `spark.sql.files.openCostInBytes`.
  * Coalesced output partitions before writing to S3.
* **Result:** Execution runtime dropped from **9m 30s to 3m 00s** on identical hardware.

### 2. End-to-End Pipeline Idempotency & Fault Tolerance
* Staging ingestion leverages atomic `TRUNCATE TABLE` operations prior to `COPY INTO`, guaranteeing zero duplicate records during task retries.
* PySpark write modes set to `overwrite` on targeted partition paths to ensure consistent states across DAG reruns.
* Automated reconciliation assertion tasks validate 100% data parity across layers:
  $$\text{Silver Staging (187,811)} = \text{Gold Fact Table (187,811)} = \text{Serving Views (187,811)}$$

### 3. Dimensional Modeling & Data Integrity
* Designed normalized dimensions (`DIM_USERS`, `DIM_MERCHANTS`, `DIM_DATE`) linked to a central `FACT_TRANSACTIONS` table.
* Handled orphan/missing entity references via surrogate default keys (`-1`) to avoid broken joins in analytical queries.

---

## 📊 Serving Layer & Dashboards

The curated views in Snowflake feed an executive dashboard in Power BI:
* `V_DAILY_FINANCIAL_SUMMARY`: Tracks Gross Merchandise Value (GMV), transaction velocity, and success rates by date.
* `V_MERCHANT_PERFORMANCE`: Ranks merchants by processed volume and identifies high-failure partners.
* `V_PAYMENT_CHANNEL_METRICS`: Evaluates channel health and conversion rates across UPI, Cards, and Net Banking.

---

## 🛠️ Tech Stack
* **Cloud Storage:** AWS S3
* **Processing:** Apache Spark (PySpark 3.x), Python
* **Data Warehouse:** Snowflake (Virtual Warehouses, Multi-Table Schemas)
* **Orchestration:** Apache Airflow 2.8+
* **Containers:** Docker, Docker Compose
* **Business Intelligence:** Power BI Desktop
* **Language:** Python, Advanced SQL (CTEs, Window Functions)