"""
Executive Portfolio Risk View Creator
=====================================
Deploys ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK view in Snowflake.
Aggregates settled AUM, Book Value, Unrealized PnL, 99% VaR, 95% PFE, and CVA by portfolio.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateExecutivePortfolioRiskView", log_file="abor_ingestion.log")


def create_executive_portfolio_risk_view(session):
    """
    Creates or replaces ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK in Snowflake.
    """
    logger.info("Deploying ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK view...")

    sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK AS
    WITH POS_SUMMARY AS (
        SELECT 
            POSITION_DATE,
            PORTFOLIO_ID,
            SUM(BOOK_VALUE) AS TOTAL_BOOK_VALUE,
            SUM(UNREALIZED_PNL) AS TOTAL_UNREALIZED_PNL,
            COUNT(DISTINCT INSTRUMENT_ID) AS ACTIVE_HOLDINGS_COUNT
        FROM ENG.FACT_DAILY_POSITIONS_ABOR
        GROUP BY POSITION_DATE, PORTFOLIO_ID
    ),
    RISK_SUMMARY AS (
        SELECT 
            METRIC_DATE,
            PORTFOLIO_ID,
            SUM(MARK_TO_MARKET) AS TOTAL_MARKET_VALUE,
            SUM(VALUE_AT_RISK_99) AS TOTAL_VAR_99,
            SUM(POTENTIAL_FUTURE_EXPOSURE) AS TOTAL_PFE_95,
            SUM(CREDIT_VALUE_ADJUSTMENT) AS TOTAL_CVA
        FROM ENG.FACT_DAILY_RISK_METRICS
        GROUP BY METRIC_DATE, PORTFOLIO_ID
    )
    SELECT 
        r.METRIC_DATE AS SNAPSHOT_DATE,
        p.PORTFOLIO_ID,
        p.PORTFOLIO_NAME,
        p.PORTFOLIO_MANAGER,
        p.INVESTMENT_STRATEGY,
        p.BASE_CURRENCY,
        pos.ACTIVE_HOLDINGS_COUNT,
        ROUND(r.TOTAL_MARKET_VALUE, 2) AS TOTAL_AUM_USD,
        ROUND(pos.TOTAL_BOOK_VALUE, 2) AS TOTAL_BOOK_VALUE_USD,
        ROUND(pos.TOTAL_UNREALIZED_PNL, 2) AS TOTAL_UNREALIZED_PNL_USD,
        ROUND(r.TOTAL_VAR_99, 2) AS TOTAL_VAR_99_USD,
        ROUND(r.TOTAL_PFE_95, 2) AS TOTAL_PFE_95_USD,
        ROUND(r.TOTAL_CVA, 2) AS TOTAL_CVA_USD,
        -- Key Risk & Financial Ratios
        ROUND((r.TOTAL_VAR_99 / NULLIF(ABS(r.TOTAL_MARKET_VALUE), 0)) * 100, 2) AS VAR_TO_AUM_PCT,
        ROUND((pos.TOTAL_UNREALIZED_PNL / NULLIF(ABS(pos.TOTAL_BOOK_VALUE), 0)) * 100, 2) AS UNREALIZED_RETURN_PCT
    FROM RISK_SUMMARY r
    JOIN ENG.DIM_PORTFOLIO p 
      ON r.PORTFOLIO_ID = p.PORTFOLIO_ID AND p.IS_ACTIVE = TRUE
    LEFT JOIN POS_SUMMARY pos 
      ON r.METRIC_DATE = pos.POSITION_DATE AND r.PORTFOLIO_ID = pos.PORTFOLIO_ID;
    """

    res = session.sql(sql).collect()
    logger.info(f"Deployed ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK successfully: {res}")
    return res
