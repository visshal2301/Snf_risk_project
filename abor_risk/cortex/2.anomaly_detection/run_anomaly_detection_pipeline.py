"""
Unified Real-Time Portfolio Risk & Asset Anomaly Detection Pipeline
===================================================================
Module: abor_risk/cortex/2.anomaly_detection
Engine: Native Snowflake Machine Learning (SNOWFLAKE.ML.ANOMALY_DETECTION)
CDC:    Snowflake Native Change Data Capture (STR_PORTFOLIO_RISK_CHANGES)

Unified Architecture:
  1. Automated Exploratory Data Analysis (EDA) & Island Gap Audit
  2. DDL Auto-Deployment:
     - Scaffolding Grid: ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT
     - Baseline Training Split: ANALYTICS.VW_PORTFOLIO_ANOMALY_TRAIN
     - Forward Evaluation Split: ANALYTICS.VW_PORTFOLIO_ANOMALY_EVAL
     - Destination Table: ANALYTICS.FACT_RISK_ANOMALIES
     - CDC Stream: ANALYTICS.STR_PORTFOLIO_RISK_CHANGES
  3. In-Database Unsupervised Multi-Series Baseline Training
  4. Dual-Mode Surveillance Execution:
     - Mode A (Batch Backfill Audit): Evaluates forward out-of-sample history
     - Mode B (Stream CDC Surveillance): Evaluates only newly arrived delta records via Stream
     - Diagnostic Simulation: --simulate-stream-event tests real-time CDC trigger and offset commit
  5. Dynamic Severity (CRITICAL, HIGH, MEDIUM) & Direction Classification
  6. Concurrency-Safe Result Persistence via Exact Query ID Pinning (sfqid)
  7. Formatted Executive Risk Surveillance Terminal Report & Alert Banners

Usage Examples:
  # 1. Full Pipeline Run (Batch Backfill Audit)
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py

  # 2. EDA Only
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py --eda-only

  # 3. Stream CDC Mode (Event-Driven Surveillance)
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py --stream-mode

  # 4. Stream Simulation (Injects synthetic event, checks stream, scores, commits offset)
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py --simulate-stream-event

  # 5. Fast Surveillance (Reuse trained model, show only breaches)
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py --skip-train --anomalies-only

  # 6. Deep-Dive on Specific Portfolio Shock
  python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py --skip-train --portfolio PORT_004
"""

import sys
import os
import uuid
import argparse
from datetime import datetime, date
from dotenv import load_dotenv

# Ensure project root is in Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from abor_risk.utils.audit_logger import AuditLogger

logger = get_logger("RiskAnomalyDetectionPipeline", log_file="abor_ingestion.log")


def run_exploratory_data_analysis(session) -> bool:
    """
    Phase 1: Automated Exploratory Data Analysis & Island Gap Audit across 20 portfolios.
    Verifies date continuity, min/max ranges, null counts, and detects historical volatility shocks.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS (EDA) & RISK DATA INTEGRITY AUDIT")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS (EDA) & RISK DATA INTEGRITY AUDIT")
    print("=" * 115)

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
        COUNT(CASE WHEN TOTAL_VAR_99_USD IS NULL THEN 1 END) AS NULL_VAR_COUNT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK;
    """
    g_stats = session.sql(global_stats_sql).collect()[0]

    logger.info(f"Time-Series Span: {g_stats['EARLIEST_DATE']} to {g_stats['LATEST_DATE']} ({g_stats['TOTAL_TRADING_DATES']} trading days)")
    logger.info(f"Observations: {g_stats['TOTAL_RECORDS']:,} rows across {g_stats['ACTIVE_PORTFOLIO_COUNT']} distinct portfolios")
    logger.info(f"99% VaR Value Range: Min ${g_stats['MIN_VAR_USD']:,.2f} | Max ${g_stats['MAX_VAR_USD']:,.2f} | Avg ${g_stats['AVG_VAR_USD']:,.2f}")
    logger.info(f"Data Health Check: Nulls={g_stats['NULL_VAR_COUNT']}")

    print(f"  * Date Span Covered          : {g_stats['EARLIEST_DATE']} to {g_stats['LATEST_DATE']} ({g_stats['TOTAL_TRADING_DATES']} trading days)")
    print(f"  * Total Observations         : {g_stats['TOTAL_RECORDS']:,} rows across {g_stats['ACTIVE_PORTFOLIO_COUNT']} distinct portfolios")
    print(f"  * 99% VaR Value Range        : Min ${g_stats['MIN_VAR_USD']:,.2f} | Max ${g_stats['MAX_VAR_USD']:,.2f} | Avg ${g_stats['AVG_VAR_USD']:,.2f}")
    print(f"  * Data Health / Defect Check : Nulls: {g_stats['NULL_VAR_COUNT']}")

    # Island Gap Audit
    island_audit_sql = """
    WITH CALENDAR_SPINE AS (
        SELECT DISTINCT SNAPSHOT_DATE FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    PORTFOLIO_SPINE AS (
        SELECT DISTINCT PORTFOLIO_ID FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    FULL_GRID AS (
        SELECT c.SNAPSHOT_DATE, p.PORTFOLIO_ID
        FROM CALENDAR_SPINE c CROSS JOIN PORTFOLIO_SPINE p
    ),
    OBSERVED AS (
        SELECT g.SNAPSHOT_DATE, g.PORTFOLIO_ID, r.TOTAL_VAR_99_USD
        FROM FULL_GRID g
        LEFT JOIN ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK r
            ON g.SNAPSHOT_DATE = r.SNAPSHOT_DATE AND g.PORTFOLIO_ID = r.PORTFOLIO_ID
    )
    SELECT 
        COUNT(*) AS TOTAL_GRID_POINTS,
        COUNT(TOTAL_VAR_99_USD) AS OBSERVED_POINTS,
        COUNT(CASE WHEN TOTAL_VAR_99_USD IS NULL THEN 1 END) AS ISLAND_GAP_COUNT
    FROM OBSERVED;
    """
    island_stats = session.sql(island_audit_sql).collect()[0]
    logger.info(f"Scaffolding Grid Points: Expected={island_stats['TOTAL_GRID_POINTS']:,} | Observed={island_stats['OBSERVED_POINTS']:,} | Gaps={island_stats['ISLAND_GAP_COUNT']}")
    print(f"  * Scaffolding Grid Points    : {island_stats['TOTAL_GRID_POINTS']:,} expected | {island_stats['OBSERVED_POINTS']:,} observed | Gaps: {island_stats['ISLAND_GAP_COUNT']}")
    if island_stats['ISLAND_GAP_COUNT'] == 0:
        logger.info("[OK] Dense time-series confirmed: Zero islands/gaps across all 20 portfolios.")
        print("    [OK] Dense time-series confirmed: Zero islands/gaps across all 20 portfolios.")
    else:
        logger.warning(f"Detected {island_stats['ISLAND_GAP_COUNT']} missing gaps. Automated LOCF gap-filling will heal them.")
        print(f"    [!] Detected {island_stats['ISLAND_GAP_COUNT']} missing gaps. Automated LOCF gap-filling will heal them.")

    # Historical Outlier Preview
    outlier_preview_sql = """
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        ROUND(TOTAL_VAR_99_USD, 2) AS TOTAL_VAR_USD,
        ROUND(TOTAL_AUM_USD, 2) AS TOTAL_AUM_USD,
        ROUND(VAR_TO_AUM_PCT, 2) AS LEVERAGE_PCT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ORDER BY TOTAL_VAR_99_USD DESC
    LIMIT 3;
    """
    rows = session.sql(outlier_preview_sql).collect()
    print("\n  * Historical Top Volatility Shocks Preview:")
    for r in rows:
        print(f"    - [{r['SNAPSHOT_DATE']}] {r['PORTFOLIO_ID']} ({r['PORTFOLIO_NAME'][:32]}): VaR=${r['TOTAL_VAR_USD']:,.2f} | Leverage={r['LEVERAGE_PCT']:.2f}%")

    logger.info("[OK - EDA PASSED] Risk time-series data integrity audit complete.")
    return True


def ensure_infrastructure_deployed(session, cortex_cfg: dict = None):
    """
    Phase 2: Deploys Scaffolding View, Training/Evaluation Splits, Destination Table, and Snowflake Stream.
    """
    cortex_cfg = cortex_cfg or {}
    train_cutoff = cortex_cfg.get("train_cutoff_date", "2026-05-31")

    logger.info("==========================================================================")
    logger.info("PHASE 2: DEPLOYING DATA INFRASTRUCTURE, VIEWS, GOLD DESTINATION TABLE & CDC STREAM")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 2: DEPLOYING DATA INFRASTRUCTURE, VIEWS, GOLD DESTINATION TABLE & CDC STREAM")
    print("=" * 115)

    # 1. Scaffolding View with Automated LOCF Forward-Filling (Resolves Island Problem)
    scaffolding_view_sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT AS
    WITH 
    CALENDAR_SPINE AS (
        SELECT DISTINCT SNAPSHOT_DATE FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    PORTFOLIO_SPINE AS (
        SELECT DISTINCT PORTFOLIO_ID FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    ),
    FULL_GRID AS (
        SELECT c.SNAPSHOT_DATE, p.PORTFOLIO_ID
        FROM CALENDAR_SPINE c CROSS JOIN PORTFOLIO_SPINE p
    ),
    OBSERVED_GRID AS (
        SELECT g.SNAPSHOT_DATE, g.PORTFOLIO_ID, r.TOTAL_VAR_99_USD
        FROM FULL_GRID g
        LEFT JOIN ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK r
            ON g.SNAPSHOT_DATE = r.SNAPSHOT_DATE AND g.PORTFOLIO_ID = r.PORTFOLIO_ID
    )
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        COALESCE(
            TOTAL_VAR_99_USD,
            LAST_VALUE(TOTAL_VAR_99_USD IGNORE NULLS) OVER (
                PARTITION BY PORTFOLIO_ID ORDER BY SNAPSHOT_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            FIRST_VALUE(TOTAL_VAR_99_USD IGNORE NULLS) OVER (
                PARTITION BY PORTFOLIO_ID ORDER BY SNAPSHOT_DATE ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) AS TOTAL_VAR_99_USD
    FROM OBSERVED_GRID;
    """
    session.sql(scaffolding_view_sql).collect()
    logger.info("[OK - VIEW] ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT deployed with dense scaffolding grid.")
    print("  [OK - VIEW] ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT deployed with dense scaffolding grid.")

    # 2. Baseline Training Split (Snapshots <= train_cutoff_date)
    train_view_sql = f"""
    CREATE OR REPLACE VIEW ANALYTICS.VW_PORTFOLIO_ANOMALY_TRAIN AS
    SELECT SNAPSHOT_DATE, PORTFOLIO_ID, TOTAL_VAR_99_USD
    FROM ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT
    WHERE SNAPSHOT_DATE <= '{train_cutoff}';
    """
    session.sql(train_view_sql).collect()

    # 3. Forward Evaluation Split (Snapshots > train_cutoff_date)
    eval_view_sql = f"""
    CREATE OR REPLACE VIEW ANALYTICS.VW_PORTFOLIO_ANOMALY_EVAL AS
    SELECT SNAPSHOT_DATE, PORTFOLIO_ID, TOTAL_VAR_99_USD
    FROM ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT
    WHERE SNAPSHOT_DATE > '{train_cutoff}';
    """
    session.sql(eval_view_sql).collect()
    logger.info(f"[OK - SPLITS] Training baseline established up to {train_cutoff}; Evaluation window is strictly post-{train_cutoff}.")
    print(f"  [OK - SPLITS] Training baseline established up to {train_cutoff}; Evaluation window is strictly post-{train_cutoff}.")

    # 4. Gold Destination Table (FACT_RISK_ANOMALIES)
    dest_table_sql = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_RISK_ANOMALIES (
        ANOMALY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        PORTFOLIO_ID VARCHAR(50) NOT NULL,
        PORTFOLIO_NAME VARCHAR(100),
        SNAPSHOT_DATE DATE NOT NULL,
        ACTUAL_VAR_99 NUMBER(38,2) NOT NULL,
        EXPECTED_VAR_99 NUMBER(38,2) NOT NULL,
        LOWER_BOUND_VAR_99 NUMBER(38,2) NOT NULL,
        UPPER_BOUND_VAR_99 NUMBER(38,2) NOT NULL,
        DISTANCE_USD NUMBER(38,2) NOT NULL,
        ANOMALY_PERCENTILE NUMBER(10,6) NOT NULL,
        IS_ANOMALY BOOLEAN NOT NULL,
        SEVERITY_LEVEL VARCHAR(20) NOT NULL,
        ANOMALY_DIRECTION VARCHAR(20) NOT NULL,
        SOURCE_MODE VARCHAR(20) DEFAULT 'BATCH',
        DETECTION_RUN_ID VARCHAR(64),
        DETECTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    session.sql(dest_table_sql).collect()
    logger.info("[OK - TABLE] Destination table ANALYTICS.FACT_RISK_ANOMALIES verified.")
    print("  [OK - TABLE] ANALYTICS.FACT_RISK_ANOMALIES verified.")

    # 5. Native Snowflake Stream (Change Data Capture on ENG.FACT_DAILY_RISK_METRICS)
    stream_sql = """
    CREATE STREAM IF NOT EXISTS ANALYTICS.STR_PORTFOLIO_RISK_CHANGES
    ON TABLE ENG.FACT_DAILY_RISK_METRICS
    APPEND_ONLY = TRUE;
    """
    session.sql(stream_sql).collect()
    logger.info("Snowflake Stream ANALYTICS.STR_PORTFOLIO_RISK_CHANGES verified (APPEND_ONLY = TRUE on ENG.FACT_DAILY_RISK_METRICS).")
    print("  [OK - STREAM] ANALYTICS.STR_PORTFOLIO_RISK_CHANGES verified (APPEND_ONLY = TRUE on ENG.FACT_DAILY_RISK_METRICS).")

    # 6. Stream Consumption Audit Log
    audit_stream_sql = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.AUDIT_STREAM_CDC_LOG (
        LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        STREAM_NAME VARCHAR(100),
        RECORDS_CONSUMED NUMBER,
        CONSUMED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    session.sql(audit_stream_sql).collect()
    logger.info("CDC Stream audit log table ANALYTICS.AUDIT_STREAM_CDC_LOG verified.")
    print("  [OK - AUDIT] ANALYTICS.AUDIT_STREAM_CDC_LOG verified.")


def train_anomaly_detection_model(session, cortex_cfg: dict = None):
    """
    Phase 3: Trains native SNOWFLAKE.ML.ANOMALY_DETECTION on baseline historical data.
    """
    cortex_cfg = cortex_cfg or {}
    model_name = cortex_cfg.get("model_name", "ANALYTICS.ML_RISK_ANOMALY_DETECTOR")
    train_cutoff = cortex_cfg.get("train_cutoff_date", "2026-05-31")

    logger.info("==========================================================================")
    logger.info(f"PHASE 3: TRAINING NATIVE SNOWFLAKE.ML.ANOMALY_DETECTION MODEL ({model_name})")
    logger.info(f"Baseline Data: ANALYTICS.VW_PORTFOLIO_ANOMALY_TRAIN (<= {train_cutoff}) | Series: PORTFOLIO_ID")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print(f"PHASE 3: TRAINING NATIVE SNOWFLAKE.ML.ANOMALY_DETECTION MODEL ({model_name})")
    print("=" * 115)
    print(f"  * Baseline Dataset      : ANALYTICS.VW_PORTFOLIO_ANOMALY_TRAIN (<= {train_cutoff})")
    print(f"  * Multi-Series Key      : PORTFOLIO_ID (20 portfolios)")
    print(f"  * Target Metric         : TOTAL_VAR_99_USD")
    print("  * Compiling Snowflake ML model in warehouse (takes ~45-60s)...")

    train_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {model_name} (
        INPUT_DATA        => TABLE(ANALYTICS.VW_PORTFOLIO_ANOMALY_TRAIN),
        SERIES_COLNAME    => 'PORTFOLIO_ID',
        TIMESTAMP_COLNAME => 'SNAPSHOT_DATE',
        TARGET_COLNAME    => 'TOTAL_VAR_99_USD',
        LABEL_COLNAME     => ''
    );
    """
    logger.info(f"Compiling Snowflake ML Anomaly Detection model {model_name}...")
    session.sql(train_sql).collect()
    logger.info(f"[OK - MODEL REGISTERED] {model_name} successfully trained and registered in Snowflake.")
    print(f"[OK - MODEL REGISTERED] {model_name} successfully trained and registered in Snowflake.")


def check_stream_cdc_status(session, cortex_cfg: dict = None) -> bool:
    """
    Checks if newly arrived risk data is waiting in the Snowflake Stream.
    Returns True if stream has data, False if idle.
    """
    cortex_cfg = cortex_cfg or {}
    stream_name = cortex_cfg.get("stream_name", "ANALYTICS.STR_PORTFOLIO_RISK_CHANGES")
    res = session.sql(f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_name}') AS HAS_DATA").collect()[0]
    has_data = bool(res['HAS_DATA'])
    logger.info(f"Snowflake Stream Status Check: '{stream_name}' -> SYSTEM$STREAM_HAS_DATA = {has_data}")
    return has_data


def execute_stream_simulation(session) -> bool:
    """
    Diagnostic Simulator:
    1. Temporarily inserts a synthetic fat-finger shock into ENG.FACT_DAILY_RISK_METRICS.
    2. Verifies that the Stream triggers (SYSTEM$STREAM_HAS_DATA becomes True).
    3. Runs CDC scoring, commits the stream offset, and cleans up the synthetic record.
    """
    logger.info("==========================================================================")
    logger.info("DIAGNOSTIC TEST: SIMULATING STREAM EVENT & REAL-TIME CDC DETECTION")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("DIAGNOSTIC TEST: SIMULATING STREAM EVENT & REAL-TIME CDC DETECTION")
    print("=" * 115)

    sim_date = "2026-10-01"
    sim_portfolio = "PORT_004"
    sim_var = 25000000.00  # $25 Million VaR fat-finger spike

    logger.info(f"Injecting synthetic shock into ENG.FACT_DAILY_RISK_METRICS for {sim_portfolio} on {sim_date} (${sim_var:,.2f} VaR)...")
    print(f"  1. Injecting synthetic shock into ENG.FACT_DAILY_RISK_METRICS for {sim_portfolio} on {sim_date} (${sim_var:,.2f} VaR)...")
    insert_sim_sql = f"""
    INSERT INTO ENG.FACT_DAILY_RISK_METRICS (
        RISK_METRIC_KEY, METRIC_DATE, PORTFOLIO_ID, COUNTERPARTY_ID, INSTRUMENT_ID,
        MARK_TO_MARKET, VALUE_AT_RISK_99, POTENTIAL_FUTURE_EXPOSURE, CREDIT_VALUE_ADJUSTMENT, CALCULATED_AT
    ) VALUES (
        9999999, '{sim_date}', '{sim_portfolio}', 'CP_SIM', 'INST_SIM',
        100000000.00, {sim_var}, 5000000.00, 100000.00, CURRENT_TIMESTAMP()
    );
    """
    session.sql(insert_sim_sql).collect()

    has_data = check_stream_cdc_status(session)
    print(f"  2. Snowflake Stream Status Check: SYSTEM$STREAM_HAS_DATA => {has_data}")
    if not has_data:
        logger.error("Stream did not detect inserted synthetic row.")
        print("  [ERROR] Stream did not detect inserted row.")
        return False
    
    logger.info("Stream detected new transaction. Calling CDC scoring pipeline...")
    print("  3. Stream detected new transaction! Calling CDC scoring pipeline...")
    execute_anomaly_detection(session, mode="stream_cdc")

    logger.info("Cleaning up synthetic test record (RISK_METRIC_KEY = 9999999)...")
    print(f"  4. Cleaning up synthetic test record (RISK_METRIC_KEY = 9999999)...")
    session.sql("DELETE FROM ENG.FACT_DAILY_RISK_METRICS WHERE RISK_METRIC_KEY = 9999999").collect()
    logger.info("[OK - SIMULATION COMPLETE] Real-time Stream CDC detection verified.")
    print("[OK - SIMULATION COMPLETE] Real-time Stream CDC detection verified.")
    return True


def execute_anomaly_detection(session, mode: str = "batch", cortex_cfg: dict = None, run_id: str = None):
    """
    Phase 4 & 5: Executes in-database anomaly detection and persists results into ANALYTICS.FACT_RISK_ANOMALIES.
    Concurrency-Safe: Pins the exact 128-bit Snowflake Cursor UUID (sfqid) to eliminate race conditions.
    """
    cortex_cfg = cortex_cfg or {}
    model_name = cortex_cfg.get("model_name", "ANALYTICS.ML_RISK_ANOMALY_DETECTOR")
    pred_interval = cortex_cfg.get("prediction_interval", 0.99)
    crit_pct = cortex_cfg.get("critical_percentile", 0.999)
    high_pct = cortex_cfg.get("high_percentile", 0.990)
    stream_name = cortex_cfg.get("stream_name", "ANALYTICS.STR_PORTFOLIO_RISK_CHANGES")
    run_id = run_id or str(uuid.uuid4())[:8]

    logger.info("==========================================================================")
    logger.info(f"PHASE 4: EXECUTING IN-DATABASE ANOMALY DETECTION (Mode: {mode.upper()})")
    logger.info(f"Prediction Interval: {int(pred_interval * 100)}% | Run ID: {run_id}")
    logger.info(f"Severity Thresholds: CRITICAL >= {crit_pct} | HIGH >= {high_pct} | MEDIUM >= 0.950")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print(f"PHASE 4: EXECUTING IN-DATABASE ANOMALY DETECTION (Mode: {mode.upper()})")
    print(f"  * Prediction Interval   : {int(pred_interval * 100)}% Volatility Envelope (from YAML)")
    print(f"  * Severity Thresholds   : CRITICAL >= {crit_pct} | HIGH >= {high_pct} | MEDIUM >= 0.950")
    print(f"  * Execution Run ID      : {run_id}")
    print("=" * 115)

    if mode == "stream_cdc":
        has_data = check_stream_cdc_status(session, cortex_cfg)
        if not has_data:
            logger.info("[STREAM IDLE] Zero new risk records in stream. SYSTEM$STREAM_HAS_DATA is FALSE. Skipping inference.")
            print("  [STREAM IDLE] Zero new risk records in stream. SYSTEM$STREAM_HAS_DATA is FALSE.")
            print("  [COST SAVINGS] Compute credits conserved. Skipping inference.")
            return 0, 0, 0, 0

        logger.info("New risk records detected in Stream. Isolating delta dates...")
        print("  [STREAM ACTIVE] New risk records detected in Stream. Isolating delta dates...")
        delta_dates_sql = f"SELECT DISTINCT METRIC_DATE FROM {stream_name}"
        dates_df = session.sql(delta_dates_sql).to_pandas()
        if dates_df.empty or len(dates_df) == 0:
            logger.info("Stream has 0 insert records. Consuming metadata offset and exiting.")
            print("  [STREAM IDLE] Stream has 0 insert records. Consuming metadata offset and exiting.")
            session.sql(f"""
                INSERT INTO ANALYTICS.AUDIT_STREAM_CDC_LOG (STREAM_NAME, RECORDS_CONSUMED)
                SELECT '{stream_name}', COUNT(*) FROM {stream_name}
            """).collect()
            return 0, 0, 0, 0

        delta_dates_list = [f"'{d.strftime('%Y-%m-%d')}'" for d in dates_df['METRIC_DATE'] if d is not None]
        if not delta_dates_list:
            logger.info("No valid dates found in stream. Exiting.")
            print("  [STREAM IDLE] No valid dates found in stream. Exiting.")
            return 0, 0, 0, 0

        logger.info(f"Delta Dates in Stream: {', '.join(delta_dates_list)}")
        print(f"  * Delta Dates in Stream: {', '.join(delta_dates_list)}")

        input_clause = f"""
        TABLE(
            SELECT SNAPSHOT_DATE, PORTFOLIO_ID, TOTAL_VAR_99_USD
            FROM ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT
            WHERE SNAPSHOT_DATE IN ({', '.join(delta_dates_list)})
        )
        """
        source_mode_tag = "STREAM_CDC"
    else:
        # Batch Backfill Mode
        input_clause = "TABLE(ANALYTICS.VW_PORTFOLIO_ANOMALY_EVAL)"
        source_mode_tag = "BATCH"

    # 1. Execute Anomaly Detection Call
    detect_sql = f"""
    CALL {model_name}!DETECT_ANOMALIES(
        INPUT_DATA        => {input_clause},
        SERIES_COLNAME    => 'PORTFOLIO_ID',
        TIMESTAMP_COLNAME => 'SNAPSHOT_DATE',
        TARGET_COLNAME    => 'TOTAL_VAR_99_USD',
        CONFIG_OBJECT     => {{'prediction_interval': {pred_interval}}}
    );
    """
    logger.info(f"Executing in-database anomaly detection CALL {model_name}!DETECT_ANOMALIES()...")
    session.sql(detect_sql).collect()
    exact_query_id = session._conn._cursor.sfqid
    logger.info(f"[OK - INFERENCE COMPLETE] Anomaly scoring complete (Query ID sfqid: {exact_query_id}).")
    print(f"[OK - INFERENCE COMPLETE] Anomaly scoring complete. Pinned Cursor UUID: {exact_query_id}")

    # 2. Idempotent cleanup of prior results for this source mode
    del_res = session.sql(f"DELETE FROM ANALYTICS.FACT_RISK_ANOMALIES WHERE SOURCE_MODE = '{source_mode_tag}'").collect()
    deleted_rows = del_res[0][0] if del_res else 0
    logger.info(f"Idempotent cleanup: removed {deleted_rows} prior records for SOURCE_MODE='{source_mode_tag}'.")

    # 3. Persist with Severity & Direction Classification using deterministic RESULT_SCAN
    persist_sql = f"""
    INSERT INTO ANALYTICS.FACT_RISK_ANOMALIES (
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
        DETECTION_RUN_ID,
        DETECTION_TIMESTAMP
    )
    WITH ANOMALY_RAW AS (
        SELECT 
            TRIM(SERIES, '"') AS PORTFOLIO_ID,
            TS::DATE AS SNAPSHOT_DATE,
            ROUND(FORECAST, 2) AS EXPECTED_VAR_99,
            ROUND(LOWER_BOUND, 2) AS LOWER_BOUND_VAR_99,
            ROUND(UPPER_BOUND, 2) AS UPPER_BOUND_VAR_99,
            IS_ANOMALY,
            ROUND(PERCENTILE, 6) AS ANOMALY_PERCENTILE,
            ROUND(DISTANCE, 2) AS DISTANCE_USD
        FROM TABLE(RESULT_SCAN('{exact_query_id}'))
    ),
    INPUT_ACTUALS AS (
        SELECT 
            SNAPSHOT_DATE,
            PORTFOLIO_ID,
            TOTAL_VAR_99_USD AS ACTUAL_VAR_99
        FROM ANALYTICS.VW_PORTFOLIO_ANOMALY_INPUT
    ),
    PORTFOLIO_NAMES AS (
        SELECT DISTINCT PORTFOLIO_ID, PORTFOLIO_NAME
        FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    )
    SELECT 
        r.PORTFOLIO_ID,
        COALESCE(p.PORTFOLIO_NAME, 'Portfolio ' || r.PORTFOLIO_ID) AS PORTFOLIO_NAME,
        r.SNAPSHOT_DATE,
        COALESCE(a.ACTUAL_VAR_99, 0.0) AS ACTUAL_VAR_99,
        r.EXPECTED_VAR_99,
        r.LOWER_BOUND_VAR_99,
        r.UPPER_BOUND_VAR_99,
        r.DISTANCE_USD,
        r.ANOMALY_PERCENTILE,
        r.IS_ANOMALY,
        CASE 
            WHEN r.IS_ANOMALY = TRUE AND r.ANOMALY_PERCENTILE >= {crit_pct} THEN 'CRITICAL'
            WHEN r.IS_ANOMALY = TRUE AND r.ANOMALY_PERCENTILE >= {high_pct} THEN 'HIGH'
            WHEN r.IS_ANOMALY = TRUE                                         THEN 'MEDIUM'
            ELSE 'NORMAL'
        END AS SEVERITY_LEVEL,
        CASE 
            WHEN r.IS_ANOMALY = TRUE AND a.ACTUAL_VAR_99 > r.EXPECTED_VAR_99 THEN 'HIGH_SPIKE'
            WHEN r.IS_ANOMALY = TRUE AND a.ACTUAL_VAR_99 < r.EXPECTED_VAR_99 THEN 'DROP'
            ELSE 'NORMAL'
        END AS ANOMALY_DIRECTION,
        '{source_mode_tag}' AS SOURCE_MODE,
        '{run_id}' AS DETECTION_RUN_ID,
        CURRENT_TIMESTAMP() AS DETECTION_TIMESTAMP
    FROM ANOMALY_RAW r
    LEFT JOIN INPUT_ACTUALS a
        ON r.PORTFOLIO_ID = a.PORTFOLIO_ID AND r.SNAPSHOT_DATE = a.SNAPSHOT_DATE
    LEFT JOIN PORTFOLIO_NAMES p
        ON r.PORTFOLIO_ID = p.PORTFOLIO_ID;
    """
    logger.info("Persisting scored records into ANALYTICS.FACT_RISK_ANOMALIES...")
    insert_res = session.sql(persist_sql).collect()
    persisted_rows = insert_res[0][0] if insert_res else 0
    logger.info(f"[OK - GOLD PERSISTED] Saved {persisted_rows} scored records into ANALYTICS.FACT_RISK_ANOMALIES.")
    print(f"[OK - GOLD PERSISTED] Scored records successfully persisted into ANALYTICS.FACT_RISK_ANOMALIES.")

    # 4. Check Anomaly Statistics
    crit_count = session.sql(f"SELECT COUNT(*) FROM ANALYTICS.FACT_RISK_ANOMALIES WHERE DETECTION_RUN_ID = '{run_id}' AND SEVERITY_LEVEL = 'CRITICAL'").collect()[0][0]
    high_count = session.sql(f"SELECT COUNT(*) FROM ANALYTICS.FACT_RISK_ANOMALIES WHERE DETECTION_RUN_ID = '{run_id}' AND SEVERITY_LEVEL = 'HIGH'").collect()[0][0]
    total_anomalies = session.sql(f"SELECT COUNT(*) FROM ANALYTICS.FACT_RISK_ANOMALIES WHERE DETECTION_RUN_ID = '{run_id}' AND IS_ANOMALY = TRUE").collect()[0][0]

    if crit_count > 0:
        logger.warning(f"[CRITICAL ANOMALIES DETECTED] Found {crit_count} CRITICAL risk anomalies (>3.5σ spikes)!")
    if high_count > 0:
        logger.warning(f"[HIGH ANOMALIES DETECTED] Found {high_count} HIGH risk anomalies!")
    logger.info(f"Anomaly Detection Summary: {total_anomalies} anomalies ({crit_count} CRITICAL, {high_count} HIGH) across {persisted_rows} evaluated records.")

    # 5. If in Stream mode, consume stream offset atomically
    if mode == "stream_cdc":
        consume_sql = f"""
        INSERT INTO ANALYTICS.AUDIT_STREAM_CDC_LOG (STREAM_NAME, RECORDS_CONSUMED)
        SELECT '{stream_name}', COUNT(*) FROM {stream_name};
        """
        session.sql(consume_sql).collect()
        logger.info("[OK - STREAM CONSUMED] Stream offset recorded into ANALYTICS.AUDIT_STREAM_CDC_LOG.")
        print(f"[OK - STREAM CONSUMED] Stream offset advanced to latest transaction timestamp.")

    return persisted_rows, crit_count, high_count, total_anomalies


def display_executive_anomaly_report(session, filter_portfolio: str = None, anomalies_only: bool = False):
    """
    Phase 6: Executive Risk Surveillance Terminal Report with Alert Banners.
    """
    print("\n" + "=" * 115)
    print("EXECUTIVE RISK SURVEILLANCE REPORT: AI-DRIVEN ANOMALY AUDIT")
    print("=" * 115)

    where_clauses = []
    if filter_portfolio:
        where_clauses.append(f"PORTFOLIO_ID = '{filter_portfolio}'")
    if anomalies_only:
        where_clauses.append("IS_ANOMALY = TRUE")
    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Summary Statistics
    summary_sql = f"""
    SELECT 
        COUNT(*) AS TOTAL_EVALUATED,
        COUNT(CASE WHEN IS_ANOMALY = TRUE THEN 1 END) AS TOTAL_ANOMALIES,
        COUNT(CASE WHEN SEVERITY_LEVEL = 'CRITICAL' THEN 1 END) AS CRITICAL_COUNT,
        COUNT(CASE WHEN SEVERITY_LEVEL = 'HIGH' THEN 1 END) AS HIGH_COUNT,
        COUNT(CASE WHEN SEVERITY_LEVEL = 'MEDIUM' THEN 1 END) AS MEDIUM_COUNT,
        COUNT(CASE WHEN ANOMALY_DIRECTION = 'HIGH_SPIKE' THEN 1 END) AS SPIKE_COUNT,
        COUNT(CASE WHEN ANOMALY_DIRECTION = 'DROP' THEN 1 END) AS DROP_COUNT
    FROM ANALYTICS.FACT_RISK_ANOMALIES
    {where_str};
    """
    summ = session.sql(summary_sql).collect()[0]

    print(f"  * Total Data Points Evaluated : {summ['TOTAL_EVALUATED']:,}")
    print(f"  * Confirmed Risk Anomalies    : {summ['TOTAL_ANOMALIES']:,}")
    print(f"  * Severity Breakdown          : CRITICAL: {summ['CRITICAL_COUNT']} | HIGH: {summ['HIGH_COUNT']} | MEDIUM: {summ['MEDIUM_COUNT']}")
    print(f"  * Outlier Dynamics            : Extreme Volatility Spikes: {summ['SPIKE_COUNT']} | Liquidity/AUM Drops: {summ['DROP_COUNT']}")

    # Critical Alert Banner (if critical anomalies exist)
    crit_sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        ACTUAL_VAR_99,
        EXPECTED_VAR_99,
        DISTANCE_USD,
        ANOMALY_PERCENTILE,
        ROUND((ACTUAL_VAR_99 / NULLIF(EXPECTED_VAR_99, 0)), 1) AS SHOCK_MULTIPLIER
    FROM ANALYTICS.FACT_RISK_ANOMALIES
    WHERE SEVERITY_LEVEL = 'CRITICAL'
    ORDER BY ACTUAL_VAR_99 DESC
    LIMIT 5;
    """
    crit_rows = session.sql(crit_sql).collect()
    if crit_rows:
        print("\n" + "!" * 115)
        print(">>> [CRITICAL RISK BREACH ALERT] IMMEDIATE EXECUTIVE ACTION REQUIRED <<<")
        print("!" * 115)
        for cr in crit_rows:
            print(
                f"  [CRITICAL BREACH] Date: {cr['SNAPSHOT_DATE']} | Portfolio: {cr['PORTFOLIO_ID']} ({cr['PORTFOLIO_NAME']})\n"
                f"    - Actual 99% VaR    : ${cr['ACTUAL_VAR_99']:,.2f}\n"
                f"    - Model Baseline VaR: ${cr['EXPECTED_VAR_99']:,.2f}\n"
                f"    - Shock Severity    : {cr['SHOCK_MULTIPLIER']}x Baseline Volatility (Outlier Distance: {cr['DISTANCE_USD']:.2f} std dev | Percentile: {cr['ANOMALY_PERCENTILE']})\n"
                f"    - Root Cause Action : FAT-FINGER TRADE / ILLIQUID ASSET SHOCK: Freeze trading & audit ticket entry."
            )
        print("!" * 115)

    # Detailed Anomaly Matrix
    detail_sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        ACTUAL_VAR_99,
        EXPECTED_VAR_99,
        SEVERITY_LEVEL,
        ANOMALY_DIRECTION,
        ANOMALY_PERCENTILE,
        SOURCE_MODE
    FROM ANALYTICS.FACT_RISK_ANOMALIES
    {where_str}
    ORDER BY IS_ANOMALY DESC, ACTUAL_VAR_99 DESC, SNAPSHOT_DATE DESC
    LIMIT 25;
    """
    rows = session.sql(detail_sql).collect()

    print("\n" + "-" * 115)
    print(f"{'DATE':<11} | {'PORTFOLIO':<10} | {'ACTUAL VaR ($)':<16} | {'EXPECTED VaR ($)':<16} | {'SEVERITY':<12} | {'DIRECTION':<12} | {'MODE'}")
    print("-" * 115)
    for r in rows:
        sev = r['SEVERITY_LEVEL']
        sev_str = f"[CRITICAL]" if sev == "CRITICAL" else (f"[HIGH]" if sev == "HIGH" else sev)
        print(
            f"{str(r['SNAPSHOT_DATE']):<11} | {r['PORTFOLIO_ID']:<10} | "
            f"${r['ACTUAL_VAR_99']:<15,.2f} | ${r['EXPECTED_VAR_99']:<15,.2f} | "
            f"{sev_str:<12} | {r['ANOMALY_DIRECTION']:<12} | {r['SOURCE_MODE']}"
        )
    print("-" * 115)
    logger.info("Executive anomaly surveillance report generated successfully.")


def parse_args():
    parser = argparse.ArgumentParser(description="Unified Real-Time Risk Anomaly Detection Pipeline")
    parser.add_argument("--config", default="config/dev.yaml", help="Path to YAML configuration")
    parser.add_argument("--eda-only", action="store_true", help="Run only Exploratory Data Analysis & Health Audit")
    parser.add_argument("--skip-train", action="store_true", help="Skip model training and reuse registered model")
    parser.add_argument("--stream-mode", action="store_true", help="Run CDC surveillance using Snowflake Stream")
    parser.add_argument("--simulate-stream-event", action="store_true", help="Diagnostic test: simulate real-time trade event in Stream")
    parser.add_argument("--anomalies-only", action="store_true", help="Filter terminal display to show only confirmed anomalies")
    parser.add_argument("--portfolio", default=None, help="Filter report by specific Portfolio ID (e.g., PORT_004)")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config=cfg)
    session = session_mgr.get_session()
    cortex_cfg = cfg.get("cortex", {}).get("anomaly_detection", {})
    exec_mode = "stream_cdc" if args.stream_mode else cortex_cfg.get("default_mode", "batch")

    audit_partition = f"{exec_mode.upper()}_{datetime.now().strftime('%Y%m%d')}"
    logger.info("**************************************************************************")
    logger.info(f"STARTING SNOWFLAKE AI/ML RISK ANOMALY DETECTION PIPELINE | Partition: {audit_partition}")
    logger.info(f"Mode: {exec_mode.upper()} | Config: {args.config} | Skip Train: {args.skip_train}")
    logger.info("**************************************************************************")

    print("\n" + "#" * 115)
    print("STARTING SNOWFLAKE AI/ML RISK ANOMALY DETECTION & SURVEILLANCE PIPELINE")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Config: {args.config} | Mode: {exec_mode.upper()}")
    print("#" * 115)

    audit_logger = AuditLogger(session, "CortexAnomalyDetectionPipeline", "RISK_ANOMALY_DETECTION", audit_partition)
    persisted_rows = 0
    crit_count = 0
    high_count = 0
    total_anomalies = 0

    try:
        session.sql(f"USE DATABASE {cfg['snowflake']['database']}").collect()
        session.sql("USE SCHEMA ANALYTICS").collect()

        # Step 1: EDA
        run_exploratory_data_analysis(session)
        if args.eda_only:
            logger.info("EDA completed successfully (--eda-only specified).")
            print("\n[OK] EDA Completed successfully.")
            audit_logger.log_success(staged_rows=0, loaded_rows=0)
            return

        # Step 2: Infrastructure DDL & Stream Deployment
        ensure_infrastructure_deployed(session, cortex_cfg=cortex_cfg)

        # Step 3: Stream Simulation (if requested)
        if args.simulate_stream_event:
            execute_stream_simulation(session)
            display_executive_anomaly_report(session, filter_portfolio=args.portfolio, anomalies_only=args.anomalies_only)
            audit_logger.log_success(staged_rows=0, loaded_rows=1, warning_count=0)
            return

        # Step 4: Model Training
        if not args.skip_train:
            train_anomaly_detection_model(session, cortex_cfg=cortex_cfg)
        else:
            logger.info(f"Reusing registered Snowflake ML model: {cortex_cfg.get('model_name')}")
            print(f"\n[SKIP-TRAIN] Reusing registered Snowflake ML model: {cortex_cfg.get('model_name')}")

        # Step 5: Scoring (Batch or Stream CDC)
        persisted_rows, crit_count, high_count, total_anomalies = execute_anomaly_detection(
            session, mode=exec_mode, cortex_cfg=cortex_cfg
        )

        # Step 6: Executive Surveillance Report
        display_executive_anomaly_report(session, filter_portfolio=args.portfolio, anomalies_only=args.anomalies_only)

        warning_count = crit_count + high_count
        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=persisted_rows,
            deleted_rows=0,
            expired_rows=0,
            warning_count=warning_count
        )

        logger.info("**************************************************************************")
        logger.info("RISK ANOMALY DETECTION PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  - Scored Records Persisted : {persisted_rows}")
        logger.info(f"  - Critical Anomalies Flagged: {crit_count}")
        logger.info(f"  - High Anomalies Flagged    : {high_count}")
        logger.info(f"  - Total Anomalies Flagged   : {total_anomalies}")
        logger.info(f"  - Audit Log Entry          : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("**************************************************************************\n")

        print("\n" + "=" * 115)
        print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN NATIVE SNOWFLAKE COMPUTE")
        print("=" * 115)

    except Exception as e:
        logger.error(f"[ANOMALY DETECTION PIPELINE FAILED] Error: {str(e)}", exc_info=True)
        print(f"\n[ERROR] Pipeline encountered an exception: {e}")
        if 'audit_logger' in locals() and audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
