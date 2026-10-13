"""
Asset Class & Security Master Risk View Creator
================================================
Deploys ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION view in Snowflake.
Aggregates Market Value, 99% VaR, PFE, and CVA sliced by Asset Class, Sub-Asset Class, and Currency.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateAssetClassRiskView", log_file="abor_ingestion.log")


def create_asset_class_risk_view(session):
    """
    Creates or replaces ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION in Snowflake.
    """
    logger.info("Deploying ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION view...")

    sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION AS
    SELECT 
        r.METRIC_DATE AS SNAPSHOT_DATE,
        i.ASSET_CLASS,
        i.SUB_ASSET_CLASS,
        i.CURRENCY,
        COUNT(DISTINCT r.PORTFOLIO_ID) AS PORTFOLIO_COUNT,
        COUNT(DISTINCT r.INSTRUMENT_ID) AS INSTRUMENTS_HELD_COUNT,
        ROUND(SUM(r.MARK_TO_MARKET), 2) AS TOTAL_MARKET_VALUE_USD,
        ROUND(SUM(r.VALUE_AT_RISK_99), 2) AS TOTAL_VAR_99_USD,
        ROUND(SUM(r.POTENTIAL_FUTURE_EXPOSURE), 2) AS TOTAL_PFE_95_USD,
        ROUND(SUM(r.CREDIT_VALUE_ADJUSTMENT), 2) AS TOTAL_CVA_USD,
        ROUND(AVG(r.VALUE_AT_RISK_99), 2) AS AVG_POSITION_VAR_USD
    FROM ENG.FACT_DAILY_RISK_METRICS r
    JOIN ENG.DIM_INSTRUMENT i 
      ON r.INSTRUMENT_ID = i.INSTRUMENT_ID AND i.IS_ACTIVE = TRUE
    GROUP BY r.METRIC_DATE, i.ASSET_CLASS, i.SUB_ASSET_CLASS, i.CURRENCY;
    """

    res = session.sql(sql).collect()
    logger.info(f"Deployed ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION successfully: {res}")
    return res
