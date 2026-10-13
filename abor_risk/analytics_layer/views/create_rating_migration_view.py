"""
Counterparty Credit Rating Migration View Creator
==================================================
Deploys ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION view in Snowflake.
Audits SCD Type 2 credit rating transitions, previous vs current ratings, and migration timestamps.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateRatingMigrationView", log_file="abor_ingestion.log")


def create_rating_migration_view(session):
    """
    Creates or replaces ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION in Snowflake.
    """
    logger.info("Deploying ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION view...")

    sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION AS
    SELECT 
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        SECTOR,
        JURISDICTION,
        CREDIT_RATING,
        EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,
        IS_ACTIVE,
        LAG(CREDIT_RATING) OVER (
            PARTITION BY COUNTERPARTY_ID 
            ORDER BY EFFECTIVE_START_DATE ASC
        ) AS PREVIOUS_RATING,
        CASE 
            WHEN LAG(CREDIT_RATING) OVER (PARTITION BY COUNTERPARTY_ID ORDER BY EFFECTIVE_START_DATE ASC) IS NULL THEN 'INITIAL_RATING'
            WHEN LAG(CREDIT_RATING) OVER (PARTITION BY COUNTERPARTY_ID ORDER BY EFFECTIVE_START_DATE ASC) = CREDIT_RATING THEN 'NO_CHANGE'
            ELSE 'RATING_MIGRATION'
        END AS MIGRATION_EVENT,
        UPDATED_AT AS RECORDED_AT
    FROM ENG.DIM_COUNTERPARTY;
    """

    res = session.sql(sql).collect()
    logger.info(f"Deployed ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION successfully: {res}")
    return res
