"""
Unified 14-Day Portfolio VaR & AUM Trajectory Forecasting Pipeline
===================================================================
Module: abor_risk/cortex/1.var_aum_trajectory
Engine: Native Snowflake Time-Series Machine Learning (SNOWFLAKE.ML.FORECAST)

Unified Workflow:
  1. Automated Exploratory Data Analysis (EDA) & Data Health Audit
  2. DDL Auto-Deployment for Forecast Persistence Table (ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS)
  3. In-Database Model Training across all 20 Portfolios (ANALYTICS.ML_PORTFOLIO_VAR_FORECAST)
  4. 14-Day Forward Inference with 95% Confidence Intervals
  5. Regulatory Risk Breach Detection & Results Persistence
  6. Formatted Terminal Executive Trajectory Report

Usage:
  python abor_risk/cortex/1.var_aum_trajectory/run_var_forecast_pipeline.py
  python abor_risk/cortex/1.var_aum_trajectory/run_var_forecast_pipeline.py --eda-only
  python abor_risk/cortex/1.var_aum_trajectory/run_var_forecast_pipeline.py --forecast-days 14
"""

import sys
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from abor_risk.utils.audit_logger import AuditLogger

logger = get_logger("VaRForecastPipeline", log_file="abor_ingestion.log")


def run_exploratory_data_analysis(session) -> bool:
    """
    Phase 1: Performs automated Exploratory Data Analysis (EDA) on the input time series.
    Verifies date continuity, min/max ranges, null checks, and multi-series completeness.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS (EDA) & DATA HEALTH AUDIT")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS (EDA) & DATA HEALTH AUDIT")
    print("=" * 115)

    # 1. Global Time-Series Span Check
    global_stats_sql = """
    SELECT 
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT PORTFOLIO_ID) AS ACTIVE_PORTFOLIO_COUNT,
        COUNT(DISTINCT SNAPSHOT_DATE) AS TOTAL_TRADING_DATES,
        MIN(SNAPSHOT_DATE) AS EARLIEST_DATE,
        MAX(SNAPSHOT_DATE) AS LATEST_DATE,
        ROUND(MIN(TOTAL_VAR_99_USD), 2) AS MIN_VAR_USD,
        ROUND(MAX(TOTAL_VAR_99_USD), 2) AS MAX_VAR_USD,
        ROUND(AVG(TOTAL_VAR_99_USD), 2) AS AVG_VAR_USD,
        COUNT(CASE WHEN TOTAL_VAR_99_USD IS NULL THEN 1 END) AS NULL_VAR_COUNT,
        COUNT(CASE WHEN TOTAL_VAR_99_USD <= 0 THEN 1 END) AS NEGATIVE_OR_ZERO_VAR_COUNT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK;
    """
    g_stats = session.sql(global_stats_sql).collect()[0]

    logger.info(f"Time-Series Span: {g_stats['EARLIEST_DATE']} to {g_stats['LATEST_DATE']} ({g_stats['TOTAL_TRADING_DATES']} trading days)")
    logger.info(f"Observations: {g_stats['TOTAL_RECORDS']:,} rows across {g_stats['ACTIVE_PORTFOLIO_COUNT']} distinct portfolios")
    logger.info(f"99% VaR Value Range: Min ${g_stats['MIN_VAR_USD']:,.2f} | Max ${g_stats['MAX_VAR_USD']:,.2f} | Avg ${g_stats['AVG_VAR_USD']:,.2f}")
    logger.info(f"Data Health Check: Nulls={g_stats['NULL_VAR_COUNT']} | Negative/Zero VaR={g_stats['NEGATIVE_OR_ZERO_VAR_COUNT']}")

    print(f"  * Date Span Covered          : {g_stats['EARLIEST_DATE']} to {g_stats['LATEST_DATE']} ({g_stats['TOTAL_TRADING_DATES']} trading days)")
    print(f"  * Total Observations         : {g_stats['TOTAL_RECORDS']:,} rows across {g_stats['ACTIVE_PORTFOLIO_COUNT']} distinct portfolios")
    print(f"  * 99% VaR Value Range        : Min ${g_stats['MIN_VAR_USD']:,.2f} | Max ${g_stats['MAX_VAR_USD']:,.2f} | Avg ${g_stats['AVG_VAR_USD']:,.2f}")
    print(f"  * Data Health / Defect Check : Nulls: {g_stats['NULL_VAR_COUNT']} | Negative/Zero VaR: {g_stats['NEGATIVE_OR_ZERO_VAR_COUNT']}")

    # 2. Island Problem / Grid Gap Audit (Date Spine vs Portfolio Observations)
    island_audit_sql = """
    WITH CALENDAR_SPINE AS (
        SELECT DISTINCT SNAPSHOT_DATE FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    PORTFOLIO_SPINE AS (
        SELECT DISTINCT PORTFOLIO_ID FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    FULL_GRID AS (
        SELECT c.SNAPSHOT_DATE, p.PORTFOLIO_ID
        FROM CALENDAR_SPINE c
        CROSS JOIN PORTFOLIO_SPINE p
    ),
    OBSERVED AS (
        SELECT 
            g.SNAPSHOT_DATE,
            g.PORTFOLIO_ID,
            r.TOTAL_VAR_99_USD
        FROM FULL_GRID g
        LEFT JOIN ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK r
            ON g.SNAPSHOT_DATE = r.SNAPSHOT_DATE
            AND g.PORTFOLIO_ID = r.PORTFOLIO_ID
    )
    SELECT 
        COUNT(*) AS TOTAL_GRID_POINTS,
        COUNT(TOTAL_VAR_99_USD) AS OBSERVED_POINTS,
        COUNT(CASE WHEN TOTAL_VAR_99_USD IS NULL THEN 1 END) AS ISLAND_GAP_COUNT
    FROM OBSERVED;
    """
    island_stats = session.sql(island_audit_sql).collect()[0]
    logger.info(f"Island Problem Audit: Expected={island_stats['TOTAL_GRID_POINTS']:,} | Observed={island_stats['OBSERVED_POINTS']:,} | Gaps={island_stats['ISLAND_GAP_COUNT']}")
    print(f"  * Island Problem Audit       : Expected Grid Points: {island_stats['TOTAL_GRID_POINTS']:,} | Observed: {island_stats['OBSERVED_POINTS']:,} | Gaps: {island_stats['ISLAND_GAP_COUNT']}")
    if island_stats['ISLAND_GAP_COUNT'] > 0:
        logger.warning(f"Detected {island_stats['ISLAND_GAP_COUNT']} missing gaps (islands). Automatic LOCF forward-filling will impute them.")
        print(f"    [!] Detected {island_stats['ISLAND_GAP_COUNT']} missing gaps (islands). Automatic LOCF forward-filling will impute them.")
    else:
        logger.info("[OK] Dense time-series confirmed: Zero islands/gaps across all 20 portfolios.")
        print("    [OK] Dense time-series confirmed: Zero islands/gaps across all 20 portfolios.")

    # 3. Per-Portfolio Continuity Breakdown
    portfolio_eda_sql = """
    SELECT 
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        COUNT(SNAPSHOT_DATE) AS DATE_COUNT,
        ROUND(MIN(TOTAL_VAR_99_USD), 2) AS MIN_VAR,
        ROUND(MAX(TOTAL_VAR_99_USD), 2) AS MAX_VAR,
        ROUND(AVG(TOTAL_VAR_99_USD), 2) AS AVG_VAR,
        ROUND(AVG(VAR_TO_AUM_PCT), 2) AS AVG_LEVERAGE_PCT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    GROUP BY PORTFOLIO_ID, PORTFOLIO_NAME
    ORDER BY PORTFOLIO_ID ASC;
    """
    rows = session.sql(portfolio_eda_sql).collect()

    print("\n" + "-" * 115)
    print(f"{'PORTFOLIO ID':<14} | {'PORTFOLIO NAME':<38} | {'DATES':<8} | {'MIN VaR ($)':<14} | {'MAX VaR ($)':<14} | {'AVG LEVERAGE'}")
    print("-" * 115)
    
    all_healthy = True
    for r in rows:
        is_complete = (r['DATE_COUNT'] == g_stats['TOTAL_TRADING_DATES'])
        if not is_complete:
            all_healthy = False
        dates_str = f"{r['DATE_COUNT']} [OK]" if is_complete else f"{r['DATE_COUNT']} [!]"
        print(
            f"{r['PORTFOLIO_ID']:<14} | {r['PORTFOLIO_NAME'][:38]:<38} | {dates_str:<8} | "
            f"${r['MIN_VAR']:<13,.2f} | ${r['MAX_VAR']:<13,.2f} | {r['AVG_LEVERAGE_PCT']:.2f}%"
        )
    print("-" * 115)

    if all_healthy and g_stats['NULL_VAR_COUNT'] == 0:
        logger.info("[OK - EDA PASSED] 100% time-series continuity confirmed across all 20 portfolios. Zero missing dates.")
        print("[OK - EDA PASSED] 100% time-series continuity confirmed across all 20 portfolios. Zero missing dates.")
        return True
    else:
        logger.warning("[WARNING - EDA AUDIT] Irregular date counts or null values detected. Proceed with caution.")
        print("[WARNING - EDA AUDIT] Irregular date counts or null values detected. Proceed with caution.")
        return False


def ensure_destination_table_exists(session):
    """
    Phase 2: Auto-deploys ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS table DDL.
    """
    ddl_sql = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS (
        FORECAST_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        PORTFOLIO_ID VARCHAR(50) NOT NULL,
        PORTFOLIO_NAME VARCHAR(100),
        MODEL_EXECUTION_DATE DATE NOT NULL,
        FORECAST_DATE DATE NOT NULL,
        HORIZON_DAY NUMBER NOT NULL,
        PROJECTED_VAR_99 NUMBER(38,2) NOT NULL,
        LOWER_BOUND_VAR_99 NUMBER(38,2) NOT NULL,
        UPPER_BOUND_VAR_99 NUMBER(38,2) NOT NULL,
        PROJECTED_AUM NUMBER(38,2),
        PROJECTED_VAR_AUM_PCT NUMBER(10,2),
        BREACH_STATUS VARCHAR(20) DEFAULT 'NORMAL',
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    session.sql(ddl_sql).collect()
    logger.info("Destination table ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS verified.")


def train_forecast_model(session, cortex_cfg: dict = None):
    """
    Phase 3: Trains native SNOWFLAKE.ML.FORECAST model on ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT.
    Applies auto-tuning lower_bound non-negative constraint from YAML configuration.
    """
    cortex_cfg = cortex_cfg or {}
    lower_bound = cortex_cfg.get("lower_bound", 0.0)

    logger.info("==========================================================================")
    logger.info("PHASE 2: TRAINING NATIVE SNOWFLAKE.ML.FORECAST MODEL (Multi-Series Across 20 Portfolios)")
    logger.info(f"Target: TOTAL_VAR_99_USD | Series: PORTFOLIO_ID | Lower Bound: {lower_bound}")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 2: TRAINING NATIVE SNOWFLAKE.ML.FORECAST MODEL (Multi-Series Across 20 Portfolios)")
    print("=" * 115)
    print("  * Training Target       : TOTAL_VAR_99_USD")
    print("  * Multi-Series Dimension: PORTFOLIO_ID")
    print("  * Historical Window     : 195 Daily Snapshots")
    print(f"  * Auto-Tuned Constraint : LOWER_BOUND => {lower_bound} (Non-negative VaR rule)")
    print("  * Executing Snowflake SQL compilation (this takes ~60-90 seconds)...")

    # 1. Create dense time-series view with automated LOCF (Last Observation Carried Forward) gap-filling.
    #    This completely solves the "Island Problem" (missing weekends, holidays, or missing portfolio snapshot days).
    prep_view_sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT AS
    WITH 
    CALENDAR_SPINE AS (
        SELECT DISTINCT SNAPSHOT_DATE
        FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    PORTFOLIO_SPINE AS (
        SELECT DISTINCT PORTFOLIO_ID
        FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    FULL_GRID AS (
        SELECT 
            c.SNAPSHOT_DATE, 
            p.PORTFOLIO_ID
        FROM CALENDAR_SPINE c
        CROSS JOIN PORTFOLIO_SPINE p
    ),
    OBSERVED_GRID AS (
        SELECT 
            g.SNAPSHOT_DATE,
            g.PORTFOLIO_ID,
            r.TOTAL_VAR_99_USD
        FROM FULL_GRID g
        LEFT JOIN ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK r
            ON g.SNAPSHOT_DATE = r.SNAPSHOT_DATE
            AND g.PORTFOLIO_ID = r.PORTFOLIO_ID
    )
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        COALESCE(
            TOTAL_VAR_99_USD,
            LAST_VALUE(TOTAL_VAR_99_USD IGNORE NULLS) OVER (
                PARTITION BY PORTFOLIO_ID 
                ORDER BY SNAPSHOT_DATE 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            FIRST_VALUE(TOTAL_VAR_99_USD IGNORE NULLS) OVER (
                PARTITION BY PORTFOLIO_ID 
                ORDER BY SNAPSHOT_DATE 
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) AS TOTAL_VAR_99_USD
    FROM OBSERVED_GRID;
    """
    logger.info("Deploying time-series input view ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT with LOCF gap-filling...")
    session.sql(prep_view_sql).collect()
    logger.info("[OK - VIEW DEPLOYED] ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT configured with automated LOCF gap-filling.")
    print("[OK - VIEW DEPLOYED] ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT configured with automated LOCF gap-filling.")

    train_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST ANALYTICS.ML_PORTFOLIO_VAR_FORECAST (
        INPUT_DATA        => TABLE(ANALYTICS.VW_PORTFOLIO_VAR_TS_INPUT),
        SERIES_COLNAME    => 'PORTFOLIO_ID',
        TIMESTAMP_COLNAME => 'SNAPSHOT_DATE',
        TARGET_COLNAME    => 'TOTAL_VAR_99_USD',
        CONFIG_OBJECT     => {{'lower_bound': {lower_bound}}}
    );
    """
    logger.info("Compiling and training SNOWFLAKE.ML.FORECAST model ANALYTICS.ML_PORTFOLIO_VAR_FORECAST in warehouse...")
    session.sql(train_sql).collect()
    logger.info("[OK - MODEL TRAINED] ANALYTICS.ML_PORTFOLIO_VAR_FORECAST successfully trained and registered in Snowflake.")
    print("[OK - MODEL TRAINED] ANALYTICS.ML_PORTFOLIO_VAR_FORECAST successfully trained and registered in Snowflake.")


def display_model_evaluation_metrics(session):
    """
    Phase 3b: Queries Snowflake's native evaluation metrics for the forecast model and formats summary.
    Concurrency-Safe: Fetches directly into Pandas DataFrame, eliminating SQL RESULT_SCAN(LAST_QUERY_ID()) race conditions.
    """
    logger.info("Fetching Snowflake ML forecast evaluation metrics via SHOW_EVALUATION_METRICS()...")
    print("\n" + "=" * 115)
    print("MODEL REGISTRY & EVALUATION METRICS (Automated Cross-Validation across 20 Portfolios)")
    print("=" * 115)
    try:
        df_raw = session.sql("CALL ANALYTICS.ML_PORTFOLIO_VAR_FORECAST!SHOW_EVALUATION_METRICS()").to_pandas()
        col_metric = [c for c in df_raw.columns if "ERROR_METRIC" in c][0]
        col_val = [c for c in df_raw.columns if "METRIC_VALUE" in c][0]

        df_metrics = df_raw.groupby(col_metric)[col_val].agg(
            AVG_VALUE="mean", MIN_VALUE="min", MAX_VALUE="max"
        ).reset_index()
        df_metrics.rename(columns={col_metric: "ERROR_METRIC"}, inplace=True)
        df_metrics.sort_values(by="ERROR_METRIC", inplace=True)

        summary_parts = []
        for _, row in df_metrics.iterrows():
            summary_parts.append(f"{row['ERROR_METRIC']}={row['AVG_VALUE']:.4f}")
        logger.info(f"Model Evaluation Summary across 20 portfolios: {', '.join(summary_parts)}")

        print(f"{'METRIC':<25} | {'AVG VALUE':<15} | {'MIN VALUE':<15} | {'MAX VALUE':<15}")
        print("-" * 80)
        for _, row in df_metrics.iterrows():
            print(f"{row['ERROR_METRIC']:<25} | {row['AVG_VALUE']:<15.4f} | {row['MIN_VALUE']:<15.4f} | {row['MAX_VALUE']:<15.4f}")
        print("-" * 80)
        print("[INFO] Model registered as schema-level object: ANALYTICS.ML_PORTFOLIO_VAR_FORECAST")
    except Exception as e:
        logger.warning(f"Could not retrieve evaluation metrics: {e}")
        print(f"[WARN] Could not retrieve evaluation metrics: {e}")


def execute_inference_and_persist(session, forecast_days: int = 14, cortex_cfg: dict = None):
    """
    Phase 4 & 5: Executes inference, runs risk breach detection from YAML limits, and saves to Gold table.
    Concurrency-Safe: Captures the exact, unique Query ID (sfqid) to avoid RESULT_SCAN(LAST_QUERY_ID()) race conditions.
    """
    cortex_cfg = cortex_cfg or {}
    pred_interval = cortex_cfg.get("prediction_interval", 0.95)
    breach_threshold = cortex_cfg.get("breach_threshold_pct", 15.0)
    elevated_threshold = cortex_cfg.get("elevated_threshold_pct", 10.0)

    logger.info("==========================================================================")
    logger.info(f"PHASE 3: GENERATING {forecast_days}-DAY FORWARD INFERENCE & RISK BREACH EVALUATION")
    logger.info(f"Prediction Interval: {int(pred_interval * 100)}% | Breach: {breach_threshold}% | Elevated: {elevated_threshold}%")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print(f"PHASE 3: GENERATING {forecast_days}-DAY FORWARD INFERENCE & RISK BREACH EVALUATION")
    print(f"  * Prediction Interval   : {int(pred_interval * 100)}% Confidence Bands (from YAML)")
    print(f"  * Regulatory Limits     : BREACH >= {breach_threshold}% VaR/AUM | ELEVATED >= {elevated_threshold}% VaR/AUM")
    print("=" * 115)

    # 1. Ensure idempotency by deleting any prior run for today's model execution date
    del_res = session.sql("DELETE FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS WHERE MODEL_EXECUTION_DATE = CURRENT_DATE()").collect()
    deleted_rows = del_res[0][0] if del_res else 0
    logger.info(f"Idempotent cleanup: removed {deleted_rows} existing forecast rows for CURRENT_DATE().")

    # 2. Execute Inference via CALL model!FORECAST
    inference_sql = f"""
    CALL ANALYTICS.ML_PORTFOLIO_VAR_FORECAST!FORECAST(
        FORECASTING_PERIODS => {forecast_days},
        CONFIG_OBJECT       => {{'prediction_interval': {pred_interval}}}
    );
    """
    logger.info(f"Executing CALL ANALYTICS.ML_PORTFOLIO_VAR_FORECAST!FORECAST({forecast_days} periods)...")
    session.sql(inference_sql).collect()
    
    # Capture the exact, unique Snowflake Query ID (UUID) immediately after execution
    exact_query_id = session._conn._cursor.sfqid
    logger.info(f"[OK - INFERENCE COMPLETE] Generated {forecast_days}-day forward forecast (Query ID sfqid: {exact_query_id}).")
    print(f"[OK - INFERENCE COMPLETE] Generated {forecast_days}-day forward forecast with {int(pred_interval * 100)}% confidence intervals (Query ID: {exact_query_id}).")

    # 3. Persist into Gold Destination Table with Breach Logic using deterministic Query ID
    persist_sql = f"""
    INSERT INTO ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS (
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
    )
    WITH FORECAST_RAW AS (
        SELECT 
            TRIM(SERIES, '"') AS PORTFOLIO_ID,
            TS::DATE AS FORECAST_DATE,
            ROW_NUMBER() OVER (PARTITION BY SERIES ORDER BY TS::DATE ASC) AS HORIZON_DAY,
            ROUND(FORECAST, 2) AS PROJECTED_VAR_99,
            ROUND(LOWER_BOUND, 2) AS LOWER_BOUND_VAR_99,
            ROUND(UPPER_BOUND, 2) AS UPPER_BOUND_VAR_99
        FROM TABLE(RESULT_SCAN('{exact_query_id}'))
    ),
    LATEST_AUM AS (
        -- Baseline current AUM to evaluate forward projected leverage ratio
        SELECT 
            PORTFOLIO_ID, 
            PORTFOLIO_NAME,
            TOTAL_AUM_USD AS CURRENT_AUM
        FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
        WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK)
    )
    SELECT 
        f.PORTFOLIO_ID,
        a.PORTFOLIO_NAME,
        CURRENT_DATE() AS MODEL_EXECUTION_DATE,
        f.FORECAST_DATE,
        f.HORIZON_DAY,
        f.PROJECTED_VAR_99,
        f.LOWER_BOUND_VAR_99,
        f.UPPER_BOUND_VAR_99,
        a.CURRENT_AUM AS PROJECTED_AUM,
        ROUND((f.PROJECTED_VAR_99 / NULLIF(a.CURRENT_AUM, 0)) * 100, 2) AS PROJECTED_VAR_AUM_PCT,
        -- Regulatory Breach Threshold Rule configured from YAML
        CASE 
            WHEN (f.UPPER_BOUND_VAR_99 / NULLIF(a.CURRENT_AUM, 0)) * 100 >= {breach_threshold} THEN 'BREACH'
            WHEN (f.PROJECTED_VAR_99 / NULLIF(a.CURRENT_AUM, 0)) * 100 >= {elevated_threshold} THEN 'ELEVATED'
            ELSE 'NORMAL'
        END AS BREACH_STATUS
    FROM FORECAST_RAW f
    JOIN LATEST_AUM a ON f.PORTFOLIO_ID = a.PORTFOLIO_ID;
    """
    logger.info("Persisting forecast predictions into ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS...")
    insert_res = session.sql(persist_sql).collect()
    persisted_rows = insert_res[0][0] if insert_res else 0
    logger.info(f"[OK - PERSISTED TO GOLD] Saved {persisted_rows} forecast rows into ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS.")
    print(f"[OK - PERSISTED TO GOLD] Saved {persisted_rows} forecast rows into ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS.")

    # 4. Check Breach Statistics
    breach_count = session.sql("SELECT COUNT(*) FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS WHERE MODEL_EXECUTION_DATE = CURRENT_DATE() AND BREACH_STATUS = 'BREACH'").collect()[0][0]
    elevated_count = session.sql("SELECT COUNT(*) FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS WHERE MODEL_EXECUTION_DATE = CURRENT_DATE() AND BREACH_STATUS = 'ELEVATED'").collect()[0][0]
    if breach_count > 0:
        logger.warning(f"[RISK BREACH DETECTED] {breach_count} forecast records flagged as BREACH (>= {breach_threshold}% VaR/AUM)!")
    if elevated_count > 0:
        logger.info(f"[ELEVATED RISK] {elevated_count} forecast records flagged as ELEVATED (>= {elevated_threshold}% VaR/AUM).")

    return persisted_rows, deleted_rows, breach_count


def print_executive_trajectory_report(session, portfolio_id: str = None):
    """
    Phase 6: Displays formatted terminal report for portfolio trajectories and alerts.
    """
    # 1. Breach and Alert High-Level Summary
    summary_sql = """
    SELECT 
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        COUNT(CASE WHEN BREACH_STATUS = 'BREACH' THEN 1 END) AS BREACH_DAYS,
        COUNT(CASE WHEN BREACH_STATUS = 'ELEVATED' THEN 1 END) AS ELEVATED_DAYS,
        ROUND(MAX(PROJECTED_VAR_AUM_PCT), 2) AS PEAK_LEVERAGE_PCT,
        ROUND(MAX(PROJECTED_VAR_99), 2) AS PEAK_VAR_USD
    FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS
    WHERE MODEL_EXECUTION_DATE = (SELECT MAX(MODEL_EXECUTION_DATE) FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS)
    GROUP BY PORTFOLIO_ID, PORTFOLIO_NAME
    HAVING BREACH_DAYS > 0 OR ELEVATED_DAYS > 0
    ORDER BY BREACH_DAYS DESC, PEAK_LEVERAGE_PCT DESC;
    """
    summary_rows = session.sql(summary_sql).collect()

    if summary_rows:
        print("\n" + "!" * 125)
        print("CRITICAL RISK BREACH & ELEVATED EXPOSURE SUMMARY (Next 14 Trading Days)")
        print("!" * 125)
        print(f"{'PORTFOLIO':<14} | {'PORTFOLIO NAME':<38} | {'BREACH DAYS':<12} | {'ELEVATED DAYS':<14} | {'PEAK VaR/AUM %':<14} | {'PEAK VaR ($)':<16} | {'STATUS'}")
        print("-" * 125)
        for s in summary_rows:
            flag = "[!] BREACH ALERT" if s['BREACH_DAYS'] > 0 else "[*] ELEVATED RISK"
            print(
                f"{s['PORTFOLIO_ID']:<14} | {s['PORTFOLIO_NAME'][:38]:<38} | "
                f"{s['BREACH_DAYS']:<12} | {s['ELEVATED_DAYS']:<14} | "
                f"{s['PEAK_LEVERAGE_PCT']:.2f}%{' ':8} | ${s['PEAK_VAR_USD']:<15,.2f} | {flag}"
            )
        print("!" * 125)

    # 2. Detailed Daily Trajectory Report
    filter_clause = f"AND PORTFOLIO_ID = '{portfolio_id}'" if portfolio_id else ""
    limit_clause = "" if portfolio_id else "LIMIT 28"
    
    report_sql = f"""
    SELECT 
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        FORECAST_DATE,
        HORIZON_DAY,
        PROJECTED_VAR_99,
        LOWER_BOUND_VAR_99,
        UPPER_BOUND_VAR_99,
        PROJECTED_VAR_AUM_PCT,
        BREACH_STATUS
    FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS
    WHERE MODEL_EXECUTION_DATE = (SELECT MAX(MODEL_EXECUTION_DATE) FROM ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS)
    {filter_clause}
    ORDER BY PORTFOLIO_ID ASC, FORECAST_DATE ASC
    {limit_clause};
    """
    rows = session.sql(report_sql).collect()

    print("\n" + "=" * 125)
    title = f"14-DAY FORWARD RISK TRAJECTORY: {portfolio_id}" if portfolio_id else "14-DAY FORWARD RISK TRAJECTORY EXECUTIVE REPORT (Sample Portfolios)"
    print(f"PHASE 4: {title}")
    print("=" * 125)
    header = (
        f"{'PORTFOLIO':<14} | {'NAME':<32} | {'DATE':<10} | {'DAY':<4} | "
        f"{'LOWER 95% ($)':<14} | {'EXPECTED VaR ($)':<16} | {'UPPER 95% ($)':<14} | {'VaR/AUM %':<9} | {'STATUS'}"
    )
    print(header)
    print("-" * 125)

    for r in rows:
        status_flag = r['BREACH_STATUS']
        if status_flag == "BREACH":
            badge = f"[!] {status_flag}"
        elif status_flag == "ELEVATED":
            badge = f"[*] {status_flag}"
        else:
            badge = f"[OK] {status_flag}"

        print(
            f"{r['PORTFOLIO_ID']:<14} | {r['PORTFOLIO_NAME'][:32]:<32} | {str(r['FORECAST_DATE']):<10} | "
            f"D+{r['HORIZON_DAY']:<2} | ${r['LOWER_BOUND_VAR_99']:<13,.2f} | ${r['PROJECTED_VAR_99']:<15,.2f} | "
            f"${r['UPPER_BOUND_VAR_99']:<13,.2f} | {r['PROJECTED_VAR_AUM_PCT']:.2f}%    | {badge}"
        )
    print("=" * 125 + "\n")
    logger.info("Executive risk trajectory terminal report generated successfully.")


def main():
    parser = argparse.ArgumentParser(description="Run Unified 14-Day VaR & AUM Forecasting Pipeline")
    parser.add_argument("--eda-only", action="store_true", help="Run only Exploratory Data Analysis audit without model training")
    parser.add_argument("--show-metrics", action="store_true", help="Display model evaluation metrics from Snowflake model registry")
    parser.add_argument("--skip-train", action="store_true", help="Skip model retraining and execute inference using existing model")
    parser.add_argument("--forecast-days", type=int, default=14, help="Number of forward forecast days (default: 14)")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file")
    parser.add_argument("--portfolio", "-p", type=str, default=None, help="Filter report to a specific portfolio ID (e.g. PORT_004)")
    args = parser.parse_args()

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()

    target_partition = datetime.now().strftime("%Y%m%d")
    logger.info("**************************************************************************")
    logger.info(f"STARTING UNIFIED 14-DAY VaR & AUM FORECASTING PIPELINE | Partition: {target_partition}")
    logger.info(f"Config: {args.config} | Forecast Days: {args.forecast_days} | Skip Train: {args.skip_train}")
    logger.info("**************************************************************************")

    audit_logger = AuditLogger(session, "CortexVaRForecastPipeline", "PORTFOLIO_RISK_FORECAST", target_partition)
    persisted_rows = 0
    deleted_rows = 0
    breach_count = 0

    try:
        session.sql(f"USE DATABASE {config['snowflake']['database']}").collect()
        session.sql("USE SCHEMA ANALYTICS").collect()

        if args.show_metrics:
            display_model_evaluation_metrics(session)
            audit_logger.log_success(staged_rows=0, loaded_rows=0)
            return

        # Phase 1: EDA
        is_healthy = run_exploratory_data_analysis(session)

        if args.eda_only:
            logger.info("--eda-only specified. Exiting without training model.")
            audit_logger.log_success(staged_rows=0, loaded_rows=0)
            return

        if not is_healthy:
            err_msg = "EDA health checks failed: irregular date counts or null values detected."
            logger.error(f"[ERROR] {err_msg} Aborting pipeline.")
            audit_logger.log_failure(err_msg)
            sys.exit(1)

        cortex_cfg = config.get("cortex", {}).get("var_forecast", {})
        forecast_days = args.forecast_days if args.forecast_days != 14 else cortex_cfg.get("forecast_days", 14)

        # Phase 2: DDL
        ensure_destination_table_exists(session)

        # Phase 3: Train Model (or skip if requested)
        if not args.skip_train:
            train_forecast_model(session, cortex_cfg=cortex_cfg)
            display_model_evaluation_metrics(session)
        else:
            logger.info("Skipping model re-training (--skip-train specified). Using existing Snowflake ML model.")

        # Phase 4 & 5: Inference & Persistence
        persisted_rows, deleted_rows, breach_count = execute_inference_and_persist(
            session, forecast_days=forecast_days, cortex_cfg=cortex_cfg
        )

        # Phase 6: Executive Terminal Display
        print_executive_trajectory_report(session, portfolio_id=args.portfolio)

        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=persisted_rows,
            deleted_rows=deleted_rows,
            expired_rows=0,
            warning_count=breach_count
        )

        logger.info("**************************************************************************")
        logger.info("VaR & AUM FORECASTING PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  - Forecast Records Persisted : {persisted_rows}")
        logger.info(f"  - Deleted Prior Records      : {deleted_rows}")
        logger.info(f"  - Breach Warnings Flagged    : {breach_count}")
        logger.info(f"  - Audit Log Entry            : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("**************************************************************************\n")

    except Exception as e:
        logger.error(f"[VaR FORECAST PIPELINE FAILED] Error: {str(e)}", exc_info=True)
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
