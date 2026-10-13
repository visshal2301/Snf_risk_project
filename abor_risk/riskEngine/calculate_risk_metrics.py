"""
Quantitative Risk Engine Orchestrator Script
==============================================
Orchestrates quantitative risk calculations by creating and invoking the
Snowflake function ENG.FN_GET_CREDIT_RISK_WEIGHT and stored procedure
ENG.SP_CALCULATE_DAILY_RISK_METRICS directly from Python.

Package Structure:
  abor_risk/riskEngine/
  ├── __init__.py
  ├── function/
  │   ├── __init__.py
  │   └── create_risk_weight_udf.py   (Creates ENG.FN_GET_CREDIT_RISK_WEIGHT UDF via session.sql)
  ├── procedure/
  │   ├── __init__.py
  │   └── create_risk_procedure.py    (Creates ENG.SP_CALCULATE_DAILY_RISK_METRICS procedure via session.sql)
  └── calculate_risk_metrics.py       (Pipeline orchestrator executing SP_CALCULATE_DAILY_RISK_METRICS)

Usage:
  python abor_risk/riskEngine/calculate_risk_metrics.py --date 2026-01-31
  python abor_risk/riskEngine/calculate_risk_metrics.py --all-dates
"""

import os
import sys
import argparse
from datetime import datetime, date
from dotenv import load_dotenv
from snowflake.snowpark.functions import col, lit

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager

from abor_risk.riskEngine.function.create_risk_weight_udf import create_fn_get_credit_risk_weight_udf
from abor_risk.riskEngine.procedure.create_risk_procedure import create_sp_calculate_daily_risk_metrics
from abor_risk.utils.audit_logger import AuditLogger

logger = get_logger("QuantitativeRiskEngine", log_file="abor_ingestion.log")


def execute_risk_metrics_calculation(session, target_date_str: str):
    """
    Executes Snowflake Stored Procedure ENG.SP_CALCULATE_DAILY_RISK_METRICS for target_date_str.
    """
    logger.info("==========================================================================")
    logger.info(f"STARTING QUANTITATIVE RISK ENGINE | Target Date: '{target_date_str}'")
    logger.info("==========================================================================")

    yyyymm = "ALL" if target_date_str.upper() == "ALL" else target_date_str.replace("-", "")[:6]
    audit_logger = AuditLogger(session, "QuantitativeRiskEngine", "RISK_METRICS", yyyymm)

    try:
        # 1. Ensure UDF and Procedure are created in Snowflake directly from Python
        logger.info("STEP 1: Registering UDF and Stored Procedure in Snowflake...")
        create_fn_get_credit_risk_weight_udf(session)
        create_sp_calculate_daily_risk_metrics(session)

        target_table = "ENG.FACT_DAILY_RISK_METRICS"

        # 2. Execute Stored Procedure
        logger.info(f"STEP 2: Executing Stored Procedure ENG.SP_CALCULATE_DAILY_RISK_METRICS('{target_date_str}')...")
        proc_result = session.call("ENG.SP_CALCULATE_DAILY_RISK_METRICS", target_date_str)
        logger.info(f"Stored Procedure Execution Output: {proc_result}")

        # 3. Data Quality Audit Verification
        logger.info("STEP 3: Executing Risk Engine Data Quality Audit Verification...")
        if target_date_str.upper() == "ALL":
            inserted_count = session.table(target_table).count()
        else:
            inserted_count = session.table(target_table).filter(col("METRIC_DATE") == lit(target_date_str)).count()
        total_count = session.table(target_table).count()

        audit_logger.log_success(staged_rows=0, loaded_rows=inserted_count, expired_rows=0)

        logger.info("==========================================================================")
        logger.info(f"QUANTITATIVE RISK ENGINE COMPLETED SUCCESSFULLY | Target Date: '{target_date_str}'")
        logger.info(f"  - Target Table                   : {target_table}")
        logger.info(f"  - Risk Ledger Rows Inserted      : {inserted_count}")
        logger.info(f"  - Total Risk Metrics Table Rows  : {total_count}")
        logger.info(f"  - Audit Log Entry                  : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("==========================================================================\n")
    except Exception as e:
        logger.error(f"[RISK ENGINE FAILED] Target Date: {target_date_str} | Error: {str(e)}")
        if 'audit_logger' in locals() and audit_logger:
            audit_logger.log_failure(str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description="Run Stored Procedure Quantitative Risk Engine")
    parser.add_argument("--date", "-d", type=str, default=None, help="Target risk calculation date (e.g., 2026-01-31).")
    parser.add_argument("--all-dates", action="store_true", help="Run risk engine across all position snapshot dates.")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file.")
    args = parser.parse_args()

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()

    try:
        session.sql(f"USE DATABASE {config['snowflake']['database']}").collect()
        session.sql("USE SCHEMA ENG").collect()

        if args.all_dates:
            execute_risk_metrics_calculation(session, "ALL")
        else:
            target_date = args.date or "2026-01-31"
            execute_risk_metrics_calculation(session, target_date)

    except Exception as e:
        logger.error(f"[RISK ENGINE FAILED] Exception encountered: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
