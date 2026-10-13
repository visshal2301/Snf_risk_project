"""
ABORT-RISK-360 Master Pipeline Orchestrator
===========================================
Executes the full end-to-end data lifecycle for ABOR Risk Analytics:
  1. Synthetic Parquet Dataset Generation (Optional via --generate-data)
  2. Pre-Ingestion Data Contract Schema Validation
  3. SCD Type 2 Dimension Ingestion (PORTFOLIO, COUNTERPARTY, INSTRUMENT)
  4. Partitioned Fact Table Ingestion (POSITIONS, TRANSACTIONS with Orphan Key Warnings)
  5. Quantitative Risk Engine Execution (Stored Procedure ENG.SP_CALCULATE_DAILY_RISK_METRICS & UDF)
  6. Consolidated Execution Audit Report (ENG.AUDIT_EXECUTION_LOG)

Usage:
  python abor_risk/main.py --yyyymm 202603 --config config/dev.yaml
  python abor_risk/main.py --yyyymm 202603 --generate-data
"""

import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager

from abor_risk.fileGeneration.generate_dim_counterparty import generate_dim_counterparty
from abor_risk.fileGeneration.generate_dim_instrument import generate_dim_instrument
from abor_risk.fileGeneration.generate_dim_portfolio import generate_dim_portfolio
from abor_risk.fileGeneration.generate_fact_positions import generate_fact_positions
from abor_risk.fileGeneration.generate_fact_transactions import generate_fact_transactions

from abor_risk.ingestion.dimensionTable.ingest_dim_portfolio import ingest_dim_portfolio
from abor_risk.ingestion.dimensionTable.ingest_dim_counterparty import ingest_dim_counterparty
from abor_risk.ingestion.dimensionTable.ingest_dim_instrument import ingest_dim_instrument
from abor_risk.ingestion.factTable.ingest_fact_positions import ingest_fact_positions
from abor_risk.ingestion.factTable.ingest_fact_transactions import ingest_fact_transactions

from abor_risk.riskEngine.calculate_risk_metrics import execute_risk_metrics_calculation

logger = get_logger("MasterPipeline", log_file="abor_ingestion.log")


def yyyymm_to_date_str(yyyymm: str) -> str:
    """Converts YYYYMM format to month-end snapshot date string YYYY-MM-DD."""
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])
    if month in (1, 3, 5, 7, 8, 10, 12):
        day = 31
    elif month in (4, 6, 9, 11):
        day = 30
    elif month == 2:
        day = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
    else:
        day = 30
    return f"{year:04d}-{month:02d}-{day:02d}"


def display_audit_summary(session, target_db: str):
    """Prints a structured summary of execution logs from ENG.AUDIT_EXECUTION_LOG."""
    logger.info("==========================================================================")
    logger.info("FETCHING MASTER PIPELINE AUDIT LOG SUMMARY (ENG.AUDIT_EXECUTION_LOG)")
    logger.info("==========================================================================")
    
    query = f"""
    SELECT 
        LOG_ID,
        PIPELINE_NAME,
        SUBJECT_AREA,
        TARGET_PARTITION,
        STATUS,
        STAGED_ROW_COUNT,
        LOADED_ROW_COUNT,
        EXPIRED_ROW_COUNT,
        WARNING_COUNT,
        DATEDIFF('second', CREATED_AT, UPDATED_AT) AS DURATION_SEC
    FROM {target_db}.ENG.AUDIT_EXECUTION_LOG
    ORDER BY LOG_ID DESC
    LIMIT 10;
    """
    rows = session.sql(query).collect()
    
    print("\n" + "=" * 105)
    print(f"MASTER PIPELINE EXECUTION AUDIT LOG SUMMARY ({target_db}.ENG.AUDIT_EXECUTION_LOG)")
    print("=" * 105)
    print(f"{'LOG ID':<8} | {'PIPELINE NAME':<24} | {'SUBJECT AREA':<22} | {'PARTITION':<12} | {'STATUS':<8} | {'LOADED':<7} | {'WARNS':<5} | {'TIME(s)':<7}")
    print("-" * 105)
    for r in reversed(rows):
        print(f"{r['LOG_ID']:<8} | {r['PIPELINE_NAME']:<24} | {r['SUBJECT_AREA']:<22} | {r['TARGET_PARTITION']:<12} | {r['STATUS']:<8} | {r['LOADED_ROW_COUNT']:<7} | {r['WARNING_COUNT']:<5} | {r['DURATION_SEC']:<7}")
    print("=" * 105 + "\n")


def run_master_pipeline(yyyymm: str, config_path: str, generate_data: bool):
    """
    Orchestrates the entire ABOR Risk platform data workflow.
    """
    logger.info("**************************************************************************")
    logger.info(f"STARTING ABORT-RISK-360 MASTER PIPELINE | Month: {yyyymm} | Config: {config_path}")
    logger.info("**************************************************************************")

    config = load_config(config_path)
    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    snapshot_date_str = yyyymm_to_date_str(yyyymm)

    # 1. Dataset Generation (Optional)
    if generate_data:
        logger.info("PHASE 1: Generating Synthetic Parquet Datasets...")
        generate_dim_counterparty()
        generate_dim_instrument()
        generate_dim_portfolio()
        generate_fact_positions()
        generate_fact_transactions()
        logger.info("Synthetic Data Generation Complete.")
    else:
        logger.info("PHASE 1: Skipping Data Generation (Using existing dataset artifacts).")

    # 2. Dimension Table Ingestion (SCD Type 2)
    logger.info("PHASE 2: Executing SCD Type 2 Dimension Ingestions...")
    logger.info("--> Ingesting DIM_PORTFOLIO...")
    ingest_dim_portfolio(yyyymm, config_path)
    
    logger.info("--> Ingesting DIM_COUNTERPARTY...")
    ingest_dim_counterparty(yyyymm, config_path)
    
    logger.info("--> Ingesting DIM_INSTRUMENT...")
    ingest_dim_instrument(yyyymm, config_path)

    # 3. Fact Table Ingestion
    logger.info("PHASE 3: Executing Fact Table Ingestions...")
    logger.info("--> Ingesting FACT_DAILY_POSITIONS_ABOR...")
    ingest_fact_positions(yyyymm=yyyymm, config_path=config_path)

    logger.info("--> Ingesting FACT_DAILY_TRANSACTIONS_ABOR...")
    ingest_fact_transactions(yyyymm=yyyymm, config_path=config_path)

    # 4. Quantitative Risk Engine Execution
    logger.info(f"PHASE 4: Executing Quantitative Risk Engine (Stored Procedure) for Snapshot Date: '{snapshot_date_str}'...")
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()
    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        session.sql("USE SCHEMA ENG").collect()
        execute_risk_metrics_calculation(session, snapshot_date_str)
        
        # 5. Display Audit Log Summary
        display_audit_summary(session, target_db)
    finally:
        session.close()

    logger.info("**************************************************************************")
    logger.info(f"ABORT-RISK-360 MASTER PIPELINE COMPLETED SUCCESSFULLY FOR MONTH '{yyyymm}'")
    logger.info("**************************************************************************")


def run_all_months_pipeline(config_path: str, generate_data: bool):
    """
    Sequentially ingests all 9 months (202601 to 202609) chronologically,
    maintaining SCD Type 2 dimension histories and loading all daily fact records.
    Then executes full risk calculations across all 185+ business dates.
    """
    logger.info("**************************************************************************")
    logger.info("STARTING ABORT-RISK-360 MASTER BATCH PIPELINE (202601 TO 202609)")
    logger.info("**************************************************************************")

    config = load_config(config_path)
    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    months = [f"20260{i}" for i in range(1, 10)]

    for m in months:
        logger.info(f"\n>>> PROCESSING BATCH MONTH: {m} <<<")
        # 1. Dimensions (SCD Type 2 chronological merges)
        logger.info(f"[{m}] Ingesting DIM_PORTFOLIO...")
        ingest_dim_portfolio(m, config_path)
        logger.info(f"[{m}] Ingesting DIM_COUNTERPARTY...")
        ingest_dim_counterparty(m, config_path)
        logger.info(f"[{m}] Ingesting DIM_INSTRUMENT...")
        ingest_dim_instrument(m, config_path)

        # 2. Fact Tables (Daily positions and transactions)
        logger.info(f"[{m}] Ingesting FACT_DAILY_POSITIONS_ABOR...")
        ingest_fact_positions(yyyymm=m, config_path=config_path)
        logger.info(f"[{m}] Ingesting FACT_SETTLED_TRANSACTIONS...")
        ingest_fact_transactions(yyyymm=m, config_path=config_path)

    # 3. Full Quantitative Risk Engine Execution across all dates
    logger.info("\n>>> EXECUTING QUANTITATIVE RISK ENGINE FOR ALL SNAPSHOT DATES <<<")
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()
    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        session.sql("USE SCHEMA ENG").collect()
        execute_risk_metrics_calculation(session, "ALL")

        # 4. Display Audit Log Summary
        display_audit_summary(session, target_db)
    finally:
        session.close()

    logger.info("**************************************************************************")
    logger.info("ABORT-RISK-360 MASTER BATCH PIPELINE COMPLETED SUCCESSFULLY (9 MONTHS)")
    logger.info("**************************************************************************")


def main():
    parser = argparse.ArgumentParser(description="ABORT-RISK-360 Master Pipeline Runner")
    parser.add_argument("--yyyymm", "-m", type=str, default="202603", help="Target month YYYYMM format (e.g. 202603)")
    parser.add_argument("--all-months", "-a", action="store_true", help="Run full pipeline across all 9 months (202601 to 202609)")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file")
    parser.add_argument("--generate-data", action="store_true", help="Generate synthetic Parquet data before running ingestion")
    args = parser.parse_args()

    if args.all_months:
        run_all_months_pipeline(args.config, args.generate_data)
    else:
        run_master_pipeline(args.yyyymm, args.config, args.generate_data)


if __name__ == "__main__":
    main()
