"""
Snowflake Quantitative Risk Stored Procedure Creator Module
============================================================
Executes Snowpark session.sql() to create/replace the ENG.SP_CALCULATE_DAILY_RISK_METRICS
stored procedure directly in Snowflake.

Supports:
  - Single date mode (batch_date_str = 'YYYY-MM-DD')
  - Full batch calculation mode (batch_date_str = 'ALL' or NULL)
  - Point-in-time SCD Type 2 join on DIM_COUNTERPARTY
"""

import sys
import os

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateRiskProcedure", log_file="abor_ingestion.log")


def create_sp_calculate_daily_risk_metrics(session):
    """
    Executes session.sql() to create/replace ENG.SP_CALCULATE_DAILY_RISK_METRICS in Snowflake.
    """
    logger.info("Executing session.sql() to create Stored Procedure: ENG.SP_CALCULATE_DAILY_RISK_METRICS...")

    sp_sql = """
    CREATE OR REPLACE PROCEDURE ENG.SP_CALCULATE_DAILY_RISK_METRICS(batch_date_str VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
    BEGIN
        IF (batch_date_str IS NULL OR UPPER(batch_date_str) = 'ALL') THEN
            TRUNCATE TABLE ENG.FACT_DAILY_RISK_METRICS;

            INSERT INTO ENG.FACT_DAILY_RISK_METRICS (
                METRIC_DATE,
                PORTFOLIO_ID,
                COUNTERPARTY_ID,
                INSTRUMENT_ID,
                MARK_TO_MARKET,
                VALUE_AT_RISK_99,
                POTENTIAL_FUTURE_EXPOSURE,
                CREDIT_VALUE_ADJUSTMENT,
                CALCULATED_AT
            )
            SELECT
                p.POSITION_DATE AS METRIC_DATE,
                p.PORTFOLIO_ID,
                p.COUNTERPARTY_ID,
                p.INSTRUMENT_ID,
                p.MARKET_VALUE AS MARK_TO_MARKET,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.1), 2) AS VALUE_AT_RISK_99,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.1) * 1.65, 2) AS POTENTIAL_FUTURE_EXPOSURE,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.65) * 0.015, 2) AS CREDIT_VALUE_ADJUSTMENT,
                CURRENT_TIMESTAMP()
            FROM ENG.FACT_DAILY_POSITIONS_ABOR p
            JOIN ENG.DIM_COUNTERPARTY c 
              ON p.COUNTERPARTY_ID = c.COUNTERPARTY_ID 
             AND p.POSITION_DATE BETWEEN c.EFFECTIVE_START_DATE AND c.EFFECTIVE_END_DATE;

            RETURN 'SUCCESS: Full Risk Metrics computed successfully for all dates.';
        ELSE
            -- 1. Partition Cleanup (Idempotency)
            DELETE FROM ENG.FACT_DAILY_RISK_METRICS 
            WHERE METRIC_DATE = CAST(:batch_date_str AS DATE);

            -- 2. Risk Calculation Insertion
            INSERT INTO ENG.FACT_DAILY_RISK_METRICS (
                METRIC_DATE,
                PORTFOLIO_ID,
                COUNTERPARTY_ID,
                INSTRUMENT_ID,
                MARK_TO_MARKET,
                VALUE_AT_RISK_99,
                POTENTIAL_FUTURE_EXPOSURE,
                CREDIT_VALUE_ADJUSTMENT,
                CALCULATED_AT
            )
            SELECT
                p.POSITION_DATE AS METRIC_DATE,
                p.PORTFOLIO_ID,
                p.COUNTERPARTY_ID,
                p.INSTRUMENT_ID,
                p.MARKET_VALUE AS MARK_TO_MARKET,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.1), 2) AS VALUE_AT_RISK_99,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.1) * 1.65, 2) AS POTENTIAL_FUTURE_EXPOSURE,
                ROUND(ABS(p.MARKET_VALUE * ENG.FN_GET_CREDIT_RISK_WEIGHT(c.CREDIT_RATING) * 1.65) * 0.015, 2) AS CREDIT_VALUE_ADJUSTMENT,
                CURRENT_TIMESTAMP()
            FROM ENG.FACT_DAILY_POSITIONS_ABOR p
            JOIN ENG.DIM_COUNTERPARTY c 
              ON p.COUNTERPARTY_ID = c.COUNTERPARTY_ID 
             AND p.POSITION_DATE BETWEEN c.EFFECTIVE_START_DATE AND c.EFFECTIVE_END_DATE
            WHERE p.POSITION_DATE = CAST(:batch_date_str AS DATE);

            RETURN 'SUCCESS: Daily Risk Metrics computed successfully for ' || :batch_date_str;
        END IF;
    END;
    $$
    """

    res = session.sql(sp_sql).collect()
    logger.info(f"Snowflake Stored Procedure Creation Result: {res}")
    print("[SUCCESS] ENG.SP_CALCULATE_DAILY_RISK_METRICS stored procedure successfully created in Snowflake!")
    return res
