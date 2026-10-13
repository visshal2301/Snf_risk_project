"""
Counterparty Concentration & Basel Large Exposure View Creator
===============================================================
Deploys ANALYTICS.VW_COUNTERPARTY_CONCENTRATION view in Snowflake.
Monitors counterparty credit exposures, rating bands, and concentration percentages against Basel regulatory thresholds.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateCounterpartyConcentrationView", log_file="abor_ingestion.log")


def create_counterparty_concentration_view(session):
    """
    Creates or replaces ANALYTICS.VW_COUNTERPARTY_CONCENTRATION in Snowflake.
    """
    logger.info("Deploying ANALYTICS.VW_COUNTERPARTY_CONCENTRATION view...")

    sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_COUNTERPARTY_CONCENTRATION AS
    WITH CPTY_RISK AS (
        SELECT 
            METRIC_DATE,
            COUNTERPARTY_ID,
            SUM(MARK_TO_MARKET) AS EXPOSURE_MTM,
            SUM(VALUE_AT_RISK_99) AS COUNTERPARTY_VAR_99,
            SUM(POTENTIAL_FUTURE_EXPOSURE) AS COUNTERPARTY_PFE_95,
            SUM(CREDIT_VALUE_ADJUSTMENT) AS COUNTERPARTY_CVA,
            COUNT(DISTINCT PORTFOLIO_ID) AS PORTFOLIO_EXPOSURE_COUNT,
            COUNT(DISTINCT INSTRUMENT_ID) AS INSTRUMENT_COUNT
        FROM ENG.FACT_DAILY_RISK_METRICS
        GROUP BY METRIC_DATE, COUNTERPARTY_ID
    ),
    TOTAL_FIRM_RISK AS (
        SELECT 
            METRIC_DATE,
            SUM(MARK_TO_MARKET) AS FIRM_TOTAL_AUM
        FROM ENG.FACT_DAILY_RISK_METRICS
        GROUP BY METRIC_DATE
    )
    SELECT 
        cr.METRIC_DATE AS SNAPSHOT_DATE,
        c.COUNTERPARTY_ID,
        c.COUNTERPARTY_NAME,
        c.LEI_CODE,
        c.SECTOR,
        c.CREDIT_RATING,
        CASE 
            WHEN c.CREDIT_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'Investment Grade'
            ELSE 'High Yield / Speculative'
        END AS RATING_CATEGORY,
        c.JURISDICTION,
        c.NETTING_AGREEMENT_TYPE,
        cr.PORTFOLIO_EXPOSURE_COUNT,
        cr.INSTRUMENT_COUNT,
        ROUND(cr.EXPOSURE_MTM, 2) AS NET_EXPOSURE_USD,
        ROUND(cr.COUNTERPARTY_VAR_99, 2) AS VAR_99_USD,
        ROUND(cr.COUNTERPARTY_PFE_95, 2) AS PFE_95_USD,
        ROUND(cr.COUNTERPARTY_CVA, 2) AS CVA_PROVISION_USD,
        -- Basel Large Exposure concentration percentage
        ROUND((cr.EXPOSURE_MTM / NULLIF(tf.FIRM_TOTAL_AUM, 0)) * 100, 2) AS FIRM_CONCENTRATION_PCT,
        -- Regulatory monitoring flag (Basel Large Exposure standard: > 10% is a large exposure warning)
        CASE 
            WHEN (cr.EXPOSURE_MTM / NULLIF(tf.FIRM_TOTAL_AUM, 0)) * 100 >= 10.0 THEN 'BREACH_WARNING'
            WHEN (cr.EXPOSURE_MTM / NULLIF(tf.FIRM_TOTAL_AUM, 0)) * 100 >= 5.0 THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS BASEL_CONCENTRATION_STATUS
    FROM CPTY_RISK cr
    JOIN TOTAL_FIRM_RISK tf ON cr.METRIC_DATE = tf.METRIC_DATE
    JOIN ENG.DIM_COUNTERPARTY c 
      ON cr.COUNTERPARTY_ID = c.COUNTERPARTY_ID AND c.IS_ACTIVE = TRUE;
    """

    res = session.sql(sql).collect()
    logger.info(f"Deployed ANALYTICS.VW_COUNTERPARTY_CONCENTRATION successfully: {res}")
    return res
