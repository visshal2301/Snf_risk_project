# 🚀 ABORT-RISK-360: End-to-End Execution Guide & Step-by-Step Operating Manual

This guide provides step-by-step instructions for running the **Institutional ABOR & Quantitative Risk Analytics Platform (ABORT-RISK-360)**.

---

## 📋 Table of Contents
1. [Prerequisites & Environment Setup](#1-prerequisites--environment-setup)
2. [Step 1: Database & Schema DDL Initialization](#step-1-database--schema-ddl-initialization)
3. [Step 2: Generate Synthetic Parquet Datasets](#step-2-generate-synthetic-parquet-datasets)
4. [Step 3: Run Pre-Ingestion Schema Contract Validation](#step-3-run-pre-ingestion-schema-contract-validation)
5. [Step 4: Execute Snowflake Ingestion Pipelines](#step-4-execute-snowflake-ingestion-pipelines)
6. [Step 5: Inspect Pipeline Logs](#step-5-inspect-pipeline-logs)
7. [Step 6: Snowflake Data Quality & Verification Audit](#step-6-snowflake-data-quality--verification-audit)

---

## 1. Prerequisites & Environment Setup

Ensure your local virtual environment is active and required Python dependencies are installed.

```bash
# 1. Activate your virtual environment
# Windows PowerShell:
.venv\Scripts\Activate.ps1

# 2. Verify required packages
pip install -r requirements.txt
```

### `.env` File Configuration
Ensure your `.env` file in the project root contains valid Snowflake credentials:

```ini
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RISK_DEV_DB
```

---

## Step 1: Database & Schema DDL Initialization

Deploy the multi-schema architecture (`RAW`, `ENG`, `ANALYTICS`) and underlying tables in Snowflake.

* **DDL Location**: [`sql/migrations/V2.0.0__abor_risk_schema.sql`](file:///d:/Padai/My_Project/Snowflake_Risk_project/sql/migrations/V2.0.0__abor_risk_schema.sql)

### Execution Options:
* **Option A (Snowflake Web UI / Worksheets)**: Copy and execute the contents of `sql/migrations/V2.0.0__abor_risk_schema.sql`.
* **Option B (SnowSQL CLI)**:
  ```bash
  snowsql -a <account> -u <user> -f sql/migrations/V2.0.0__abor_risk_schema.sql
  ```

---

## Step 2: Generate Synthetic Parquet Datasets

Generate 9 months of synthetic data (`202601` to `202609`) with deliberate credit rating migrations, taxonomy reclassifications, and out-of-order variations.

```bash
# 1. Generate Counterparty Dimension Parquet files
python abor_risk/fileGeneration/generate_dim_counterparty.py

# 2. Generate Instrument Dimension Parquet files
python abor_risk/fileGeneration/generate_dim_instrument.py

# 3. Generate Portfolio Dimension Parquet files
python abor_risk/fileGeneration/generate_dim_portfolio.py
```

### Target File Output Directory:
* `abor_risk/outputFile/COUNTERPARTY/DATA_YYYYMM/*.parquet`
* `abor_risk/outputFile/INSTRUMENT/DATA_YYYYMM/*.parquet`
* `abor_risk/outputFile/PORTFOLIO/DATA_YYYYMM/*.parquet`

---

## Step 3: Run Pre-Ingestion Schema Contract Validation

Validate generated Parquet files against strict JSON metadata schemas (`abor_risk/metadata/*_schema.json`) before initiating Snowflake ingestion.

```bash
python abor_risk/fileGeneration/schema_validator.py
```

> **Governance Policy**:
> * Missing column or data type mismatch ➡️ **ABORT** (Pipeline fails fast locally).
> * Additional columns present ➡️ **PASS** (Allowed under additive schema evolution rules).

---

## Step 4: Execute Snowflake Ingestion Pipelines

Run the modular Snowpark ingestion scripts. These scripts automate staging via transient tables, perform SCD Type 2 out-of-order temporal alignment, enforce data uniqueness checks, and log all SQL actions.

```bash
# 1. Ingest Counterparty Dimension (SCD Type 2)
python abor_risk/ingestion/dimensionTable/ingest_dim_counterparty.py

# 2. Ingest Instrument Dimension (SCD Type 2)
python abor_risk/ingestion/dimensionTable/ingest_dim_instrument.py

# 3. Ingest Portfolio Dimension (SCD Type 2)
python abor_risk/ingestion/dimensionTable/ingest_dim_portfolio.py
```

---

## Step 5: Inspect Pipeline Logs

All operations, step-by-step progress, raw SQL queries, and audit statistics are appended to a persistent log file.

* **Log Location**: [`logs/abor_ingestion.log`](file:///d:/Padai/My_Project/Snowflake_Risk_project/logs/abor_ingestion.log)

```bash
# View log output in real time (PowerShell / Bash)
Get-Content logs/abor_ingestion.log -Tail 50 -Wait
```

---

## Step 6: Snowflake Data Quality & Verification Audit

Run the following SQL queries in Snowflake to verify table counts, SCD Type 2 temporal history, and ensure zero duplicate active records (`IS_ACTIVE = TRUE`).

```sql
USE DATABASE RISK_DEV_DB;
USE SCHEMA ENG;

-- 1. Counterparty Active Record Uniqueness Audit
SELECT COUNTERPARTY_ID, COUNT(*) AS ACTIVE_DUPLICATES
FROM DIM_COUNTERPARTY
WHERE IS_ACTIVE = TRUE
GROUP BY COUNTERPARTY_ID
HAVING COUNT(*) > 1;

-- 2. Instrument Active Record Uniqueness Audit
SELECT INSTRUMENT_ID, COUNT(*) AS ACTIVE_DUPLICATES
FROM DIM_INSTRUMENT
WHERE IS_ACTIVE = TRUE
GROUP BY INSTRUMENT_ID
HAVING COUNT(*) > 1;

-- 3. Portfolio Active Record Uniqueness Audit
SELECT PORTFOLIO_ID, COUNT(*) AS ACTIVE_DUPLICATES
FROM DIM_PORTFOLIO
WHERE IS_ACTIVE = TRUE
GROUP BY PORTFOLIO_ID
HAVING COUNT(*) > 1;

-- 4. View Sample SCD Type 2 Temporal Migration (Counterparty Credit Ratings over Time)
SELECT COUNTERPARTY_ID, COUNTERPARTY_NAME, CREDIT_RATING, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_ACTIVE
FROM DIM_COUNTERPARTY
ORDER BY COUNTERPARTY_ID, EFFECTIVE_START_DATE ASC;
```

---

## Step 7: Execute Fact Table Ingestion (Positions & Transactions)

Ingest daily accounting positions and settled transactions into the Silver layer (`ENG` schema).

```bash
# Ingest single month via modular scripts
python abor_risk/ingestion/factTable/ingest_fact_positions.py --year 2026 --month 1
python abor_risk/ingestion/factTable/ingest_fact_transactions.py --year 2026 --month 1

# Or run the master ABOR orchestrator (processes all dimensions + facts)
python abor_risk/main.py --year 2026 --month 1
```

---

## Step 8: Calculate In-Database Daily Quantitative Risk Metrics

Execute the Snowflake Stored Procedure `SP_CALCULATE_DAILY_RISK_METRICS` to compute 99% VaR, 95% PFE, and CVA directly inside Snowflake warehouse compute:

```bash
# Calculate metrics for a single date
python abor_risk/riskEngine/calculate_risk_metrics.py --date 2026-09-25

# Or backfill an entire month
python abor_risk/riskEngine/calculate_risk_metrics.py --backfill-month 202609
```

---

## Step 9: Deploy Analytics Consumption Layer Views

Build the 5 Gold analytical views and dimensional marts in the `ANALYTICS` schema:

```bash
python abor_risk/analytics_layer/deploy_analytics_layer.py
```

Inspect and print the live executive reports:
```bash
python abor_risk/analytics_layer/fetch_analytics_reports.py
```

---

## Step 10: Execute the 3 Cortex AI / ML Quantitative Risk Modules

Run the three native Snowflake machine learning and graph simulation pipelines:

```bash
# 1. Use Case 1: VaR Anomaly & Backtesting Breach Detector
python abor_risk/cortex/1.var_anomaly_detector/run_var_anomaly_pipeline.py

# 2. Use Case 2: Systemic Stress Contagion & Liquidity Shock Simulator
python abor_risk/cortex/2.liquidity_shock_simulator/run_liquidity_shock_pipeline.py

# 3. Use Case 3: Counterparty Downgrade Early Warning (Full Orchestrator)
python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py

# Or run Use Case 3 stages independently:
python abor_risk/cortex/3.counterparty_downgrade_predictor/1_train_downgrade_model.py
python abor_risk/cortex/3.counterparty_downgrade_predictor/2_validate_downgrade_model.py
python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py
```

---

## Step 11: Launch & Deploy Streamlit in Snowflake (SiS) Risk Command Center

Deploy the interactive executive dashboard visualizing portfolio metrics, forward VaR trajectories, Eisenberg-Noe domino contagion cascades, and counterparty downgrade radars.

### Option A: Run Locally via Streamlit
```bash
streamlit run abor_risk/streamlit/risk_dashboard.py
```

### Option B: Deploy Natively into Snowflake (Streamlit in Snowflake - SiS)
```bash
python abor_risk/streamlit/deploy_sis.py
```
*(Or execute `abor_risk/streamlit/deploy_sis.sql` in SnowSQL or Snowflake Worksheets).*

Once deployed, open **Snowflake Web UI** $\to$ **Projects** $\to$ **Streamlit** $\to$ **`RISK_COMMAND_CENTER`**.

---

## 📌 Complete Workflow Execution Summary Matrix

| Step | Script / Command | Target Output / Action |
| :--- | :--- | :--- |
| **DDL Migration** | `sql/migrations/V2.0.0__abor_risk_schema.sql` | Builds `RAW`, `ENG`, `ANALYTICS` schemas |
| **Data Gen: Dimensions** | `python abor_risk/fileGeneration/generate_dim_*.py` | Produces `202601`–`202609` Parquet files |
| **Data Gen: Facts** | `python abor_risk/fileGeneration/generate_fact_*.py` | Produces daily position & transaction Parquet files |
| **Contract Validation** | `python abor_risk/fileGeneration/schema_validator.py` | Enforces JSON schema contracts |
| **Ingest: Dimensions** | `python abor_risk/ingestion/dimensionTable/ingest_dim_*.py` | Loads SCD2 dimensions into `ENG.DIM_*` |
| **Ingest: Facts** | `python abor_risk/main.py --year 2026 --month 1` | Loads `ENG.FACT_DAILY_POSITIONS_ABOR` & `FACT_SETTLED_TRANSACTIONS` |
| **Risk Engine** | `python abor_risk/riskEngine/calculate_risk_metrics.py` | Runs SPROC to populate `ENG.FACT_DAILY_RISK_METRICS` |
| **Analytics Layer** | `python abor_risk/analytics_layer/deploy_analytics_layer.py` | Deploys 5 Gold executive views in `ANALYTICS` |
| **Cortex Use Case 1** | `python abor_risk/cortex/1.var_aum_trajectory/run_var_forecast_pipeline.py` | Populates `ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS` |
| **Cortex Use Case 2** | `python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py` | Populates `ANALYTICS.FACT_RISK_ANOMALIES` |
| **Cortex Use Case 3** | `python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py` | Populates `ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS` |
| **Streamlit SiS UI** | `python abor_risk/streamlit/deploy_sis.py` | Deploys native UI app `ANALYTICS.RISK_COMMAND_CENTER` |
| **Audit Logs** | `logs/abor_ingestion.log` & `ENG.AUDIT_EXECUTION_LOG` | Comprehensive file and database audit trail |
