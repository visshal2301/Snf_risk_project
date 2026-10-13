"""
Snowflake Credit Risk Weight UDF Creator Module
=================================================
Executes Snowpark session.sql() to create/replace the ENG.FN_GET_CREDIT_RISK_WEIGHT UDF directly in Snowflake.

SQL Definition:
  CREATE OR REPLACE FUNCTION ENG.FN_GET_CREDIT_RISK_WEIGHT(rating VARCHAR)
  RETURNS FLOAT
  LANGUAGE SQL
  AS
  $$
    (CASE rating
      WHEN 'AAA' THEN 0.04
      WHEN 'AA'  THEN 0.06
      WHEN 'A'   THEN 0.09
      WHEN 'BBB' THEN 0.13
      WHEN 'BB'  THEN 0.22
      WHEN 'B'   THEN 0.28
      ELSE 0.35
    END)::FLOAT
  $$;
"""

import sys
import os

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("CreateRiskWeightUDF", log_file="abor_ingestion.log")


def create_fn_get_credit_risk_weight_udf(session):
    """
    Executes session.sql() to create ENG.FN_GET_CREDIT_RISK_WEIGHT UDF in Snowflake.
    """
    logger.info("Executing session.sql() to create UDF: ENG.FN_GET_CREDIT_RISK_WEIGHT...")

    udf_sql = """
    CREATE OR REPLACE FUNCTION ENG.FN_GET_CREDIT_RISK_WEIGHT(rating VARCHAR)
    RETURNS FLOAT
    LANGUAGE SQL
    AS
    $$
      (CASE rating
        WHEN 'AAA' THEN 0.04
        WHEN 'AA'  THEN 0.06
        WHEN 'A'   THEN 0.09
        WHEN 'BBB' THEN 0.13
        WHEN 'BB'  THEN 0.22
        WHEN 'B'   THEN 0.28
        ELSE 0.35
      END)::FLOAT
    $$
    """

    res = session.sql(udf_sql).collect()
    logger.info(f"Snowflake UDF Creation Result: {res}")
    print("[SUCCESS] ENG.FN_GET_CREDIT_RISK_WEIGHT UDF successfully created in Snowflake!")
    return res
