"""
Data Access & Snowpark Session Service for Streamlit
====================================================
Provides dual-mode connection support:
  1. Streamlit in Snowflake (SiS) native context: get_active_session()
  2. Local development: SnowflakeSessionManager with .env and config/dev.yaml
"""

import os
import sys
import decimal
from typing import Optional, List, Dict, Any
from pathlib import Path
import pandas as pd
import streamlit as st

# Ensure repository root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def to_clean_dataframe(rows) -> pd.DataFrame:
    """
    Converts Snowpark Row objects into a clean pandas DataFrame, coercing
    Snowflake Decimal types into standard numeric floats for full compatibility
    with Plotly and pandas operations.
    """
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([r.as_dict() if hasattr(r, "as_dict") else r for r in rows])
    for col in df.columns:
        if df[col].dtype == object and not df[col].empty:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, decimal.Decimal):
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_resource
def get_session():
    """
    Acquires an active Snowflake session.
    Automatically detects whether running natively inside Streamlit in Snowflake (SiS)
    or locally via Python virtualenv.
    """
    try:
        # 1. Native Streamlit in Snowflake (SiS)
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session
    except Exception:
        # 2. Local Fallback via Snowpark SessionManager
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
        from src.utils.config_loader import load_config
        from src.connection.session import SnowflakeSessionManager

        cfg = load_config(PROJECT_ROOT / "config" / "dev.yaml")
        session_mgr = SnowflakeSessionManager(config=cfg)
        return session_mgr.get_session()


@st.cache_data(ttl=300)
def get_available_snapshot_dates() -> List[str]:
    """Returns sorted list of distinct snapshot dates in descending order."""
    session = get_session()
    sql = """
    SELECT DISTINCT SNAPSHOT_DATE 
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK 
    ORDER BY SNAPSHOT_DATE DESC;
    """
    rows = session.sql(sql).collect()
    return [str(r['SNAPSHOT_DATE']) for r in rows] if rows else ["2026-09-30"]


@st.cache_data(ttl=300)
def get_portfolio_list() -> List[Dict[str, str]]:
    """Returns list of active portfolios with IDs and names."""
    session = get_session()
    sql = """
    SELECT PORTFOLIO_ID, PORTFOLIO_NAME, PORTFOLIO_MANAGER, INVESTMENT_STRATEGY 
    FROM ENG.DIM_PORTFOLIO 
    WHERE IS_ACTIVE = TRUE 
    ORDER BY PORTFOLIO_ID ASC;
    """
    rows = session.sql(sql).collect()
    return [r.as_dict() for r in rows] if rows else []


@st.cache_data(ttl=300)
def get_executive_portfolio_metrics(snapshot_date: str, portfolio_id: Optional[str] = None) -> pd.DataFrame:
    """Retrieves executive portfolio risk metrics for a specific date."""
    session = get_session()
    clause = f"AND PORTFOLIO_ID = '{portfolio_id}'" if portfolio_id and portfolio_id != "ALL" else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_MANAGER,
        INVESTMENT_STRATEGY,
        BASE_CURRENCY,
        ACTIVE_HOLDINGS_COUNT,
        TOTAL_AUM_USD,
        TOTAL_BOOK_VALUE_USD,
        TOTAL_UNREALIZED_PNL_USD,
        TOTAL_VAR_99_USD,
        TOTAL_PFE_95_USD,
        TOTAL_CVA_USD,
        VAR_TO_AUM_PCT,
        UNREALIZED_RETURN_PCT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    WHERE SNAPSHOT_DATE = '{snapshot_date}' {clause}
    ORDER BY TOTAL_AUM_USD DESC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_asset_class_distribution(snapshot_date: str) -> pd.DataFrame:
    """Retrieves asset class and tenor risk breakdown."""
    session = get_session()
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        ASSET_CLASS,
        SUB_ASSET_CLASS,
        CURRENCY,
        PORTFOLIO_COUNT,
        INSTRUMENTS_HELD_COUNT,
        TOTAL_MARKET_VALUE_USD,
        TOTAL_VAR_99_USD,
        TOTAL_PFE_95_USD,
        TOTAL_CVA_USD,
        AVG_POSITION_VAR_USD
    FROM ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION
    WHERE SNAPSHOT_DATE = '{snapshot_date}'
    ORDER BY TOTAL_MARKET_VALUE_USD DESC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_var_forecasts(portfolio_id: Optional[str] = None) -> pd.DataFrame:
    """Retrieves 14-day forward VaR trajectory forecast data."""
    session = get_session()
    clause = f"WHERE PORTFOLIO_ID = '{portfolio_id}'" if portfolio_id and portfolio_id != "ALL" else ""
    sql = f"""
    SELECT 
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        MODEL_EXECUTION_DATE,
        FORECAST_DATE,
        HORIZON_DAY,
        PROJECTED_VAR_99,
        LOWER_BOUND_VAR_99,
        UPPER_BOUND_VAR_99,
        PROJECTED_AUM,
        PROJECTED_VAR_AUM_PCT,
        BREACH_STATUS
    FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS
    {clause}
    ORDER BY FORECAST_DATE ASC, PORTFOLIO_ID ASC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_counterparty_concentration(snapshot_date: str, sector: Optional[str] = None) -> pd.DataFrame:
    """Retrieves counterparty credit exposures and Basel large exposure statuses."""
    session = get_session()
    clause = f"AND c.SECTOR = '{sector}'" if sector and sector != "ALL" else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        LEI_CODE,
        SECTOR,
        CREDIT_RATING,
        RATING_CATEGORY,
        JURISDICTION,
        NETTING_AGREEMENT_TYPE,
        PORTFOLIO_EXPOSURE_COUNT,
        INSTRUMENT_COUNT,
        NET_EXPOSURE_USD,
        VAR_99_USD,
        PFE_95_USD,
        CVA_PROVISION_USD,
        FIRM_CONCENTRATION_PCT,
        BASEL_CONCENTRATION_STATUS
    FROM ANALYTICS.VW_COUNTERPARTY_CONCENTRATION c
    WHERE SNAPSHOT_DATE = '{snapshot_date}' {clause}
    ORDER BY NET_EXPOSURE_USD DESC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_downgrade_predictions(snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """Retrieves ML Counterparty Credit Rating Downgrade early warning scores."""
    session = get_session()
    clause = f"WHERE SNAPSHOT_DATE = '{snapshot_date}'" if snapshot_date else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        SECTOR,
        JURISDICTION,
        CURRENT_CREDIT_RATING,
        DOWNGRADE_PROBABILITY,
        PREDICTED_DOWNGRADE_FLAG,
        RISK_TIER,
        RECOMMENDED_ACTION,
        CONFIDENCE_SCORE,
        FEATURE_NET_EXPOSURE_USD,
        FEATURE_CONCENTRATION_PCT,
        FEATURE_MTM_VOLATILITY_14D,
        FEATURE_TRADE_COUNT_30D,
        FEATURE_RATING_TENURE_DAYS,
        MODEL_VERSION
    FROM ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS
    {clause}
    ORDER BY DOWNGRADE_PROBABILITY DESC, FEATURE_NET_EXPOSURE_USD DESC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_rating_migrations() -> pd.DataFrame:
    """Retrieves SCD Type 2 counterparty rating migration audit trail."""
    session = get_session()
    sql = """
    SELECT 
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        SECTOR,
        JURISDICTION,
        CREDIT_RATING,
        EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,
        IS_ACTIVE,
        PREVIOUS_RATING,
        MIGRATION_EVENT,
        RECORDED_AT
    FROM ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION
    ORDER BY COUNTERPARTY_ID, EFFECTIVE_START_DATE ASC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_risk_anomalies(portfolio_id: Optional[str] = None, severity: Optional[str] = None) -> pd.DataFrame:
    """Retrieves unsupervised risk anomaly detections and outlier breaches."""
    session = get_session()
    clauses = []
    if portfolio_id and portfolio_id != "ALL":
        clauses.append(f"PORTFOLIO_ID = '{portfolio_id}'")
    if severity and severity != "ALL":
        clauses.append(f"SEVERITY_LEVEL = '{severity}'")
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    sql = f"""
    SELECT 
        ANOMALY_ID,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        SNAPSHOT_DATE,
        ACTUAL_VAR_99,
        EXPECTED_VAR_99,
        LOWER_BOUND_VAR_99,
        UPPER_BOUND_VAR_99,
        DISTANCE_USD,
        ANOMALY_PERCENTILE,
        IS_ANOMALY,
        SEVERITY_LEVEL,
        ANOMALY_DIRECTION,
        SOURCE_MODE,
        DETECTION_TIMESTAMP
    FROM ANALYTICS.FACT_RISK_ANOMALIES
    {where_sql}
    ORDER BY SNAPSHOT_DATE DESC, DISTANCE_USD DESC;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_audit_logs(limit: int = 15) -> pd.DataFrame:
    """Retrieves enterprise pipeline execution audit logs."""
    session = get_session()
    sql = f"""
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
        EXECUTION_DURATION_SEC,
        CREATED_AT,
        UPDATED_AT
    FROM ENG.AUDIT_EXECUTION_LOG
    ORDER BY LOG_ID DESC
    LIMIT {limit};
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)


@st.cache_data(ttl=300)
def get_model_registry_info() -> pd.DataFrame:
    """Retrieves registered in-database ML model metadata."""
    session = get_session()
    sql = """
    SELECT 
        MODEL_NAME,
        MODEL_VERSION,
        OPTIMAL_THRESHOLD,
        ROC_AUC,
        TRAIN_ROWS,
        CREATED_AT
    FROM ANALYTICS.FACT_MODEL_STORE;
    """
    rows = session.sql(sql).collect()
    return to_clean_dataframe(rows)
