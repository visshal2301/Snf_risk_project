"""
FACT_SETTLED_TRANSACTIONS Ingestion Pipeline Script
=====================================================
Validates landing Parquet schema contract, checks orphan key thresholds against active dimensions,
persists execution audits to ENG.AUDIT_EXECUTION_LOG, and executes idempotent deletion and insertion
from raw Parquet staging into ENG.FACT_SETTLED_TRANSACTIONS inside Snowflake.

Deletion / Partitioning Logic:
  - If daily (YYYYMMDD): Deletes target partition WHERE TRANSACTION_DATE = 'YYYY-MM-DD'
  - If monthly (YYYYMM): Deletes target partition WHERE DATE_TRUNC('month', TRANSACTION_DATE) = 'YYYY-MM-01'

Usage:
  python abor_risk/ingestion/factTable/ingest_fact_transactions.py --yyyymm 202603 --config config/dev.yaml
  python abor_risk/ingestion/factTable/ingest_fact_transactions.py --yyyymmdd 20260315 --config config/dev.yaml
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

logger = get_logger("IngestTransactions", log_file="abor_ingestion.log")


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


def ingest_fact_transactions(yyyymm: str = None, yyyymmdd: str = None, config_path: str = "config/dev.yaml"):
    """
    Executes pre-ingestion contract validation, orphan key warning check,
    Snowflake staging upload, idempotent deletion and insertion into ENG.FACT_SETTLED_TRANSACTIONS,
    and audit log persistence into ENG.AUDIT_EXECUTION_LOG.
    """
    subject_area = "TRANSACTIONS"

    if yyyymmdd:
        mode_label = f"Daily ({yyyymmdd})"
        target_partition = yyyymmdd
        month_folder = yyyymmdd[:6]
        formatted_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
        file_name = f"TRANSACTIONS_DAILY_{yyyymmdd}.parquet"
    else:
        if not yyyymm:
            yyyymm = get_default_last_month()
        mode_label = f"Monthly ({yyyymm})"
        target_partition = yyyymm
        month_folder = yyyymm
        file_name = f"TRANSACTIONS_MONTHLY_{yyyymm}.parquet"

    logger.info("==========================================================================")
    logger.info(f"STARTING FACT INGESTION PIPELINE | Subject Area: '{subject_area}' | Mode: {mode_label}")
    logger.info("==========================================================================")

    # 1. Locate Source Parquet File & Metadata Contract
    parquet_file_path = os.path.join(
        PROJECT_ROOT, "abor_risk", "outputFile", subject_area, f"DATA_{month_folder}", file_name
    )
    metadata_dir = os.path.join(PROJECT_ROOT, "abor_risk", "metadata")

    logger.info(f"STEP 1: Locating input file artifact...")
    logger.info(f"  - Target Parquet Path : {parquet_file_path}")
    logger.info(f"  - Metadata Config Dir : {metadata_dir}")

    if not os.path.exists(parquet_file_path):
        raise FileNotFoundError(f"Input Parquet file not found at: '{parquet_file_path}'")

    # 2. Pre-Ingestion Data Contract Validation
    logger.info("STEP 2: Running Pre-Ingestion Schema Contract Validation...")
    validate_parquet_schema(parquet_file_path, subject_area, metadata_dir, logger=logger)

    # 3. Establish Snowflake Connection & Initialize Audit Logger
    logger.info("STEP 3: Connecting to Snowflake Warehouse & Engine...")
    config = load_config(config_path)
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()

    audit_logger = AuditLogger(session, "IngestTransactions", subject_area, target_partition)
    warning_count = 0

    try:
        target_db = config["snowflake"]["database"]
        raw_schema = "RAW"
        eng_schema = "ENG"
        stage_name = "@RAW.STAGE_ABOR_RISK"

        execute_and_log_sql(session, f"USE DATABASE {target_db}", "Switching database context")
        execute_and_log_sql(session, f"USE SCHEMA {raw_schema}", "Switching context to RAW schema")

        # 4. Upload Parquet File to Stage
        snowflake_stage_path = f"{stage_name}/{subject_area}/{month_folder}/"
        logger.info(f"STEP 4: Uploading Parquet file to Snowflake stage ({snowflake_stage_path})...")
        clean_path = parquet_file_path.replace("\\", "/")
        put_sql = f"PUT file://{clean_path} {snowflake_stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        execute_and_log_sql(session, put_sql, "Staging Parquet File via PUT command")

        # 5. Truncate Transient Staging Table & Copy Parquet Data
        staging_table = f"{raw_schema}.STG_SETTLED_TRANSACTIONS_DELTA"
        logger.info(f"STEP 5: Truncating & Bulk Loading Raw Staging Table ({staging_table})...")

        execute_and_log_sql(session, f"TRUNCATE TABLE {staging_table}", "Truncate transient staging table")

        copy_sql = f"""
        COPY INTO {staging_table} 
        FROM {snowflake_stage_path}{file_name}
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """
        execute_and_log_sql(session, copy_sql, f"Bulk Loading Parquet Stage Data to {staging_table}")

        stg_count = session.sql(f"SELECT COUNT(*) FROM {staging_table}").collect()[0][0]
        logger.info(f"Transient Staging Load Complete | {staging_table} Row Count: {stg_count}")

        # 6. Orphan Key Check Against Active Dimensions with Threshold Warning
        logger.info("STEP 6: Checking Orphan Keys against active dimension tables...")
        orphan_port_count = session.sql(f"""
            SELECT COUNT(*) FROM {staging_table} s
            LEFT JOIN {eng_schema}.DIM_PORTFOLIO d ON s.PORTFOLIO_ID = d.PORTFOLIO_ID AND d.IS_ACTIVE = TRUE
            WHERE d.PORTFOLIO_ID IS NULL
        """).collect()[0][0]

        orphan_cpty_count = session.sql(f"""
            SELECT COUNT(*) FROM {staging_table} s
            LEFT JOIN {eng_schema}.DIM_COUNTERPARTY d ON s.COUNTERPARTY_ID = d.COUNTERPARTY_ID AND d.IS_ACTIVE = TRUE
            WHERE d.COUNTERPARTY_ID IS NULL
        """).collect()[0][0]

        orphan_inst_count = session.sql(f"""
            SELECT COUNT(*) FROM {staging_table} s
            LEFT JOIN {eng_schema}.DIM_INSTRUMENT d ON s.INSTRUMENT_ID = d.INSTRUMENT_ID AND d.IS_ACTIVE = TRUE
            WHERE d.INSTRUMENT_ID IS NULL
        """).collect()[0][0]

        total_orphan_count = orphan_port_count + orphan_cpty_count + orphan_inst_count
        orphan_threshold = config.get("data_quality", {}).get("orphan_warning_threshold", 0)

        if total_orphan_count > orphan_threshold:
            logger.warning(
                f"[ORPHAN KEY WARNING] Found {total_orphan_count} orphan key occurrences in staging "
                f"(Portfolios: {orphan_port_count}, Counterparties: {orphan_cpty_count}, Instruments: {orphan_inst_count}). "
                f"Exceeds threshold of {orphan_threshold}."
            )
            warning_count += 1
        else:
            logger.info(f"[DATA QUALITY PASSED] Orphan key check verified (Total orphans: {total_orphan_count}).")

        # 7. Idempotent Partition Deletion & Insertion
        target_table = f"{eng_schema}.FACT_SETTLED_TRANSACTIONS"
        logger.info(f"STEP 7: Executing Idempotent Partition Deletion & Insertion into {target_table}...")

        if yyyymmdd:
            delete_sql = f"DELETE FROM {target_table} WHERE TRANSACTION_DATE = '{formatted_date}'::DATE"
            delete_desc = f"Idempotent deletion of daily partition ({formatted_date})"
        else:
            year_val = yyyymm[:4]
            month_val = yyyymm[4:6]
            bom_date = f"{year_val}-{month_val}-01"
            delete_sql = f"DELETE FROM {target_table} WHERE DATE_TRUNC('month', TRANSACTION_DATE) = '{bom_date}'::DATE"
            delete_desc = f"Idempotent deletion of monthly partition ({bom_date})"

        del_result = execute_and_log_sql(session, delete_sql, delete_desc)
        deleted_count = del_result[0][0] if del_result else 0

        insert_sql = f"""
        INSERT INTO {target_table} (
            TRANSACTION_ID,
            TRANSACTION_DATE,
            SETTLEMENT_DATE,
            PORTFOLIO_ID,
            COUNTERPARTY_ID,
            INSTRUMENT_ID,
            TRANSACTION_TYPE,
            QUANTITY,
            PRICE,
            SETTLED_AMOUNT,
            CURRENCY,
            INGESTED_AT
        )
        SELECT 
            TRANSACTION_ID,
            TRANSACTION_DATE,
            SETTLEMENT_DATE,
            PORTFOLIO_ID,
            COUNTERPARTY_ID,
            INSTRUMENT_ID,
            TRANSACTION_TYPE,
            QUANTITY,
            PRICE,
            SETTLED_AMOUNT,
            CURRENCY,
            CURRENT_TIMESTAMP()
        FROM {staging_table}
        """
        ins_result = execute_and_log_sql(session, insert_sql, f"Inserting staged transactions into {target_table}")
        inserted_count = ins_result[0][0] if ins_result else 0

        # 8. Data Quality Audit Checks
        logger.info(f"STEP 8: Running Data Quality Audit Checks on {target_table}...")
        fact_count = session.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

        bad_amt_count = session.sql(f"""
            SELECT COUNT(*) 
            FROM {target_table} 
            WHERE ABS(SETTLED_AMOUNT - (QUANTITY * PRICE)) > 0.05
        """).collect()[0][0]

        if bad_amt_count > 0:
            raise ValueError(f"[DQ AUDIT FAILED] Found {bad_amt_count} rows with invalid SETTLED_AMOUNT calculation!")
        logger.info("[DATA QUALITY PASSED] Settled Amount math integrity verified.")

        # 9. Persist Execution Audit to ENG.AUDIT_EXECUTION_LOG
        audit_logger.log_success(
            staged_rows=stg_count,
            loaded_rows=inserted_count,
            deleted_rows=deleted_count,
            warning_count=warning_count
        )

        logger.info("==========================================================================")
        logger.info(f"INGESTION PIPELINE COMPLETED SUCCESSFULLY | Subject Area: '{subject_area}' ({mode_label})")
        logger.info(f"  - Database                   : {target_db}")
        logger.info(f"  - Total Table Rows           : {fact_count}")
        logger.info(f"  - Staged Partition Loaded    : {stg_count}")
        logger.info(f"  - Audit Log Entry            : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("==========================================================================\n")

    except Exception as e:
        logger.error(f"[PIPELINE FAILED] Exception encountered during '{subject_area}' ingestion: {str(e)}")
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest FACT_SETTLED_TRANSACTIONS Parquet to Snowflake ENG schema")
    parser.add_argument("--yyyymm", "-m", type=str, default=None, help="Target month in YYYYMM format (e.g. 202603).")
    parser.add_argument("--yyyymmdd", "-d", type=str, default=None, help="Target date in YYYYMMDD format (e.g. 20260315).")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file.")
    args = parser.parse_args()

    ingest_fact_transactions(yyyymm=args.yyyymm, yyyymmdd=args.yyyymmdd, config_path=args.config)
