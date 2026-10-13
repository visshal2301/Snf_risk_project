"""
Trade Turnover & Broker Execution View Creator
===============================================
Deploys ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS view in Snowflake.
Measures traded volumes, transaction counts, buy/sell ratios, and broker execution volume from FACT_SETTLED_TRANSACTIONS.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateTradeTurnoverView", log_file="abor_ingestion.log")


def create_trade_turnover_view(session):
    """
    Creates or replaces ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS in Snowflake.
    """
    logger.info("Deploying ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS view...")

    sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS AS
    SELECT 
        t.TRANSACTION_DATE,
        t.SETTLEMENT_DATE,
        p.PORTFOLIO_NAME,
        p.PORTFOLIO_MANAGER,
        c.COUNTERPARTY_NAME AS BROKER_NAME,
        c.CREDIT_RATING AS BROKER_RATING,
        i.ASSET_CLASS,
        t.TRANSACTION_TYPE,
        t.CURRENCY,
        COUNT(t.TRANSACTION_ID) AS TOTAL_TRADES_COUNT,
        SUM(t.QUANTITY) AS TOTAL_UNITS_TRADED,
        ROUND(SUM(t.SETTLED_AMOUNT), 2) AS TOTAL_VOLUME_USD,
        ROUND(AVG(t.PRICE), 4) AS AVG_EXECUTION_PRICE
    FROM ENG.FACT_SETTLED_TRANSACTIONS t
    JOIN ENG.DIM_PORTFOLIO p 
      ON t.PORTFOLIO_ID = p.PORTFOLIO_ID AND p.IS_ACTIVE = TRUE
    JOIN ENG.DIM_COUNTERPARTY c 
      ON t.COUNTERPARTY_ID = c.COUNTERPARTY_ID AND c.IS_ACTIVE = TRUE
    JOIN ENG.DIM_INSTRUMENT i 
      ON t.INSTRUMENT_ID = i.INSTRUMENT_ID AND i.IS_ACTIVE = TRUE
    GROUP BY 
        t.TRANSACTION_DATE,
        t.SETTLEMENT_DATE,
        p.PORTFOLIO_NAME,
        p.PORTFOLIO_MANAGER,
        c.COUNTERPARTY_NAME,
        c.CREDIT_RATING,
        i.ASSET_CLASS,
        t.TRANSACTION_TYPE,
        t.CURRENCY;
    """

    res = session.sql(sql).collect()
    logger.info(f"Deployed ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS successfully: {res}")
    return res
