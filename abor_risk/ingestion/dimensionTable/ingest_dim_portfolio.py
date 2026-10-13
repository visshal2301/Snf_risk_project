"""
DIM_PORTFOLIO Ingestion Pipeline Script
========================================
Validates landing Parquet schema contract and executes idempotent SCD Type 2
ingestion from raw Parquet staging into ENG.DIM_PORTFOLIO inside Snowflake.
Logs all detailed pipeline execution steps and raw SQL statements to logs/abor_ingestion.log.

Output / Target:
  Target Table: ENG.DIM_PORTFOLIO (Database driven by config/dev.yaml or config/prod.yaml)

Usage:
  python abor_risk/ingestion/dimensionTable/ingest_dim_portfolio.py --yyyymm 202609 --config config/dev.yaml
  python abor_risk/ingestion/dimensionTable/ingest_dim_portfolio.py (defaults to last month)

TODO Roadmap for Generic SCD2 Framework:
  1. Abstract business key (e.g., PORTFOLIO_ID, COUNTERPARTY_ID, INSTRUMENT_ID) into metadata/config.
  2. Abstract surrogate key (e.g., PORTFOLIO_KEY, COUNTERPARTY_KEY, INSTRUMENT_KEY) into metadata/config.
  3. Dynamic tracked attribute list comparison for triggering SCD2 updates.
  4. Parameterized single-statement MERGE SQL generator supporting any target/staging table pair.
  5. Reusable post-MERGE timeline re-alignment utility (LEAD / ROW_NUMBER calculations).
  6. Reusable Data Quality audit assertion runner for active ID uniqueness.
"""

import os
import sys
import argparse
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from abor_risk.fileGeneration.schema_validator import validate_parquet_schema
from abor_risk.utils.audit_logger import AuditLogger

logger = get_logger("IngestPortfolio", log_file="abor_ingestion.log")


def get_default_last_month() -> str:
    """Calculates last month in YYYYMM format based on current local date."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_date = first_of_this_month - timedelta(days=1)
    return last_month_date.strftime("%Y%m")


def execute_and_log_sql(session, sql_statement: str, description: str):
    """Utility function to log the exact SQL query before execution for debugging."""
    logger.info(f"--- [SQL EXECUTION: {description}] ---")
    logger.info(f"SQL Query:\n{sql_statement.strip()}\n")
    result = session.sql(sql_statement).collect()
    logger.info(f"SQL Result: {result}")
    return result


def ingest_dim_portfolio(yyyymm: str, config_path: str):
    """
    Executes pre-ingestion contract validation, Snowflake staging upload,
    and idempotent SCD Type 2 MERGE into ENG.DIM_PORTFOLIO.
    """
    subject_area = "PORTFOLIO"
    logger.info("==========================================================================")
    logger.info(f"STARTING DIMENSION INGESTION PIPELINE | Subject Area: '{subject_area}' | Target Month: '{yyyymm}'")
    logger.info("==========================================================================")

    # 1. Locate Source Parquet File & Metadata Contract
    parquet_file_path = os.path.join(
        PROJECT_ROOT, "abor_risk", "outputFile", subject_area, f"DATA_{yyyymm}", f"dim_portfolio_{yyyymm}.parquet"
    )
    metadata_dir = os.path.join(PROJECT_ROOT, "abor_risk", "metadata")

    logger.info(f"STEP 1: Locating input file artifact...")
    logger.info(f"  - Source Parquet File Path: {parquet_file_path}")
    logger.info(f"  - Metadata Schema Directory: {metadata_dir}")

    if not os.path.exists(parquet_file_path):
        logger.error(f"[CRITICAL ERROR] Source Parquet file not found at path: {parquet_file_path}")
        logger.error("Action: Aborting ingestion. Please run the file generator script first.")
        sys.exit(1)

    # 2. Pre-Ingestion Data Contract Schema Validation
    logger.info(f"STEP 2: Executing Data Contract Schema Pre-Validation...")
    validate_parquet_schema(parquet_file_path, subject_area, metadata_dir, logger=logger)

    # 3. Load Snowflake Configuration & Establish Session
    logger.info(f"STEP 3: Loading Snowflake environment configuration from '{config_path}'...")
    config = load_config(config_path)
    session = SnowflakeSessionManager(config).get_session()

    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    logger.info(f"Target Database: '{target_db}' | Warehouse: '{config.get('snowflake', {}).get('warehouse')}'")

    audit_logger = AuditLogger(session, "IngestPortfolio", subject_area, yyyymm)

    try:
        execute_and_log_sql(session, f"USE DATABASE {target_db}", "Switching context to target database")
        execute_and_log_sql(session, "USE SCHEMA RAW", "Switching context to RAW schema")

        # 4. Upload Parquet File to Stage & Load Transient Staging Table
        logger.info(f"STEP 4: Uploading Parquet file to Snowflake stage...")
        stage_path = f"@RAW.STAGE_ABOR_RISK/{subject_area}/{yyyymm}/"
        formatted_file_path = parquet_file_path.replace("\\", "/")
        put_cmd = f"PUT file://{formatted_file_path} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        execute_and_log_sql(session, put_cmd, "Staging Parquet File via PUT command")

        logger.info("STEP 5: Truncating & Bulk Loading Raw Delta Staging Table (RAW.STG_PORTFOLIO_DELTA)...")
        execute_and_log_sql(session, "TRUNCATE TABLE RAW.STG_PORTFOLIO_DELTA", "Truncate transient staging table")

        copy_cmd = f"""
        COPY INTO RAW.STG_PORTFOLIO_DELTA 
        FROM {stage_path}dim_portfolio_{yyyymm}.parquet
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        execute_and_log_sql(session, copy_cmd, "Bulk Loading Parquet Stage Data to RAW.STG_PORTFOLIO_DELTA")

        stg_count = session.sql("SELECT COUNT(*) FROM RAW.STG_PORTFOLIO_DELTA").collect()[0][0]
        logger.info(f"Transient Staging Load Complete | RAW.STG_PORTFOLIO_DELTA Row Count: {stg_count}")

        # 5. Execute Single-Statement Unified SCD Type 2 MERGE into ENG.DIM_PORTFOLIO
        logger.info("STEP 6: Executing Single-Statement Unified SCD Type 2 MERGE into ENG.DIM_PORTFOLIO...")

        unified_merge_scd2_sql = """
        MERGE INTO ENG.DIM_PORTFOLIO AS target
        USING (
            -- Branch 1: Matching active target records to expire (UPDATE driver)
            SELECT 
                'UPDATE' AS ACTION_TYPE,
                PORTFOLIO_ID AS JOIN_KEY,
                PORTFOLIO_ID,
                PORTFOLIO_NAME,
                BASE_CURRENCY,
                PORTFOLIO_MANAGER,
                INVESTMENT_STRATEGY,
                INCEPTION_DATE,
                EFFECTIVE_START_DATE,
                EFFECTIVE_END_DATE,
                IS_ACTIVE
            FROM RAW.STG_PORTFOLIO_DELTA
            WHERE IS_ACTIVE = TRUE

            UNION ALL

            -- Branch 2: Inserting new active versions & new entities (INSERT driver with NULL JOIN_KEY)
            SELECT 
                'INSERT' AS ACTION_TYPE,
                NULL AS JOIN_KEY,
                s.PORTFOLIO_ID,
                s.PORTFOLIO_NAME,
                s.BASE_CURRENCY,
                s.PORTFOLIO_MANAGER,
                s.INVESTMENT_STRATEGY,
                s.INCEPTION_DATE,
                s.EFFECTIVE_START_DATE,
                s.EFFECTIVE_END_DATE,
                s.IS_ACTIVE
            FROM RAW.STG_PORTFOLIO_DELTA s
            LEFT JOIN ENG.DIM_PORTFOLIO t
              ON s.PORTFOLIO_ID = t.PORTFOLIO_ID
             AND s.EFFECTIVE_START_DATE = t.EFFECTIVE_START_DATE
            WHERE t.PORTFOLIO_ID IS NULL
        ) AS source
        ON target.PORTFOLIO_ID = source.JOIN_KEY
       AND target.IS_ACTIVE = TRUE
        WHEN MATCHED AND source.ACTION_TYPE = 'UPDATE' AND (
            target.PORTFOLIO_MANAGER <> source.PORTFOLIO_MANAGER OR
            target.INVESTMENT_STRATEGY <> source.INVESTMENT_STRATEGY
        ) THEN
            UPDATE SET 
                target.EFFECTIVE_END_DATE = DATEADD(day, -1, source.EFFECTIVE_START_DATE),
                target.IS_ACTIVE = FALSE,
                target.UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED AND source.ACTION_TYPE = 'INSERT' THEN
            INSERT (
                PORTFOLIO_ID,
                PORTFOLIO_NAME,
                BASE_CURRENCY,
                PORTFOLIO_MANAGER,
                INVESTMENT_STRATEGY,
                INCEPTION_DATE,
                EFFECTIVE_START_DATE,
                EFFECTIVE_END_DATE,
                IS_ACTIVE,
                UPDATED_AT
            ) VALUES (
                source.PORTFOLIO_ID,
                source.PORTFOLIO_NAME,
                source.BASE_CURRENCY,
                source.PORTFOLIO_MANAGER,
                source.INVESTMENT_STRATEGY,
                source.INCEPTION_DATE,
                source.EFFECTIVE_START_DATE,
                source.EFFECTIVE_END_DATE,
                source.IS_ACTIVE,
                CURRENT_TIMESTAMP()
            );
        """
        execute_and_log_sql(session, unified_merge_scd2_sql, "Unified SCD Type 2 MERGE (Expiring Old Active Versions & Inserting New Versions)")

        realign_timeline_sql = """
        UPDATE ENG.DIM_PORTFOLIO AS target
        SET 
            target.EFFECTIVE_END_DATE = CASE 
                WHEN t.NEXT_START_DATE IS NOT NULL THEN DATEADD(day, -1, t.NEXT_START_DATE)
                ELSE '9999-12-31'::DATE
            END,
            target.IS_ACTIVE = CASE 
                WHEN t.RN = 1 THEN TRUE 
                ELSE FALSE 
            END,
            target.UPDATED_AT = CURRENT_TIMESTAMP()
        FROM (
            SELECT 
                PORTFOLIO_KEY,
                PORTFOLIO_ID,
                EFFECTIVE_START_DATE,
                LEAD(EFFECTIVE_START_DATE) OVER (
                    PARTITION BY PORTFOLIO_ID 
                    ORDER BY EFFECTIVE_START_DATE ASC, PORTFOLIO_KEY ASC
                ) AS NEXT_START_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY PORTFOLIO_ID 
                    ORDER BY EFFECTIVE_START_DATE DESC, PORTFOLIO_KEY DESC
                ) AS RN
            FROM ENG.DIM_PORTFOLIO
        ) t
        WHERE target.PORTFOLIO_KEY = t.PORTFOLIO_KEY;
        """
        execute_and_log_sql(session, realign_timeline_sql, "Re-aligning Out-of-Order SCD Type 2 Timelines & Active Flags")

        # STEP 7: Data Quality Check - Active Portfolio ID Uniqueness Audit
        logger.info("STEP 7: Running Active ID Uniqueness Data Quality Audit on ENG.DIM_PORTFOLIO...")
        dq_sql = """
        SELECT PORTFOLIO_ID, COUNT(*) AS active_count
        FROM ENG.DIM_PORTFOLIO
        WHERE IS_ACTIVE = TRUE
        GROUP BY PORTFOLIO_ID
        HAVING COUNT(*) > 1;
        """
        duplicates = session.sql(dq_sql).collect()
        if duplicates:
            dup_ids = [row[0] for row in duplicates]
            logger.error(f"[DATA QUALITY FAILURE] Duplicate active records found in ENG.DIM_PORTFOLIO for ID(s): {dup_ids}")
            sys.exit(1)
        
        logger.info("[DATA QUALITY PASSED] Active PORTFOLIO_ID uniqueness verified (0 duplicates found).")

        active_count = session.sql("SELECT COUNT(*) FROM ENG.DIM_PORTFOLIO WHERE IS_ACTIVE = TRUE").collect()[0][0]
        expired_count = session.sql("SELECT COUNT(*) FROM ENG.DIM_PORTFOLIO WHERE IS_ACTIVE = FALSE").collect()[0][0]
        total_count = session.sql("SELECT COUNT(*) FROM ENG.DIM_PORTFOLIO").collect()[0][0]

        audit_logger.log_success(
            staged_rows=stg_count,
            loaded_rows=total_count,
            expired_rows=expired_count
        )

        logger.info("==========================================================================")
        logger.info(f"INGESTION PIPELINE COMPLETED SUCCESSFULLY | Subject Area: '{subject_area}' ({yyyymm})")
        logger.info(f"  - Database               : {target_db}")
        logger.info(f"  - ENG.DIM_PORTFOLIO Total Rows     : {total_count}")
        logger.info(f"  - Active Versions (IS_ACTIVE=TRUE)  : {active_count}")
        logger.info(f"  - Expired SCD2 Versions (IS_ACTIVE=F): {expired_count}")
        logger.info(f"  - Audit Log Entry                  : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("==========================================================================\n")

    except Exception as e:
        logger.error(f"[PIPELINE FAILED] Exception encountered during '{subject_area}' ingestion: {str(e)}")
        if 'audit_logger' in locals() and audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest DIM_PORTFOLIO Parquet to Snowflake ENG schema")
    parser.add_argument(
        "--yyyymm", "-m",
        type=str,
        default=None,
        help="Target month in YYYYMM format (e.g., 202609). Defaults to last month if omitted."
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        default="config/dev.yaml",
        help="Path to YAML configuration file (default: config/dev.yaml)"
    )
    args = parser.parse_args()

    target_month = args.yyyymm if args.yyyymm else get_default_last_month()
    ingest_dim_portfolio(target_month, args.config)
