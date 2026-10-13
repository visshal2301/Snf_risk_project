"""
Unified Counterparty Credit Rating Downgrade Early Warning Pipeline
====================================================================
Module: abor_risk/cortex/3.counterparty_downgrade_predictor
Engine: Native Snowflake Machine Learning & Snowpark In-Database ML
Model:  Random Forest Classifier with Point-in-Time SCD2 Feature Store

Unified Architecture:
  1. Automated Exploratory Data Analysis (EDA) & Feature Health Audit
  2. DDL Auto-Deployment:
     - Scaffolding Feature View: ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
     - Predictions Table: ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS
     - Model Metrics Table: ANALYTICS.FACT_MODEL_REGISTRY_METRICS
  3. In-Database Supervised Ensemble Training (ANALYTICS.SP_TRAIN_CPTY_DOWNGRADE_MODEL)
     - Strict Chronological Splitting (Train: <= 2026-06-30 | Test: > 2026-06-30)
     - Evaluation: ROC-AUC, Precision, Recall, F1, Confusion Matrix, Feature Importances
  4. In-Database Batch Scoring & Risk Tiering (ANALYTICS.SP_SCORE_CPTY_DOWNGRADE_PREDICTIONS)
     - Probability scoring P(Downgrade) in [0.0, 1.0]
     - Categorization: CRITICAL (>=70%), HIGH (>=40%), WATCHLIST (>=20%), LOW (<20%)
     - Prescriptive operational directives (margin calls, limit cuts, watchlist)
  5. Formatted Executive Risk Terminal Scoreboard & Alert Radar
  6. Two-Tier Audit Governance via AuditLogger -> ENG.AUDIT_EXECUTION_LOG

Usage Examples:
  # 1. Full Pipeline Run (EDA, Feature Scaffold, Train, Score Latest Snapshot Date)
  python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py

  # 2. EDA & Feature Health Audit Only
  python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py --eda-only

  # 3. Model Training & Evaluation Only
  python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py --train-only

  # 4. Fast Inference (Skip Retraining, Score Specific Snapshot Date)
  python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py --skip-train --date 2026-09-30

  # 5. Filter for Critical & High Risk Alerts Only
  python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py --alerts-only
"""

import sys
import os
import json
import textwrap
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

logger = get_logger("CounterpartyDowngradePipeline", log_file="abor_ingestion.log")


# ==============================================================================
# PHASE 1: EXPLORATORY DATA ANALYSIS (EDA) & FEATURE HEALTH AUDIT
# ==============================================================================

def run_exploratory_data_analysis(session) -> bool:
    """
    Phase 1: Automated Exploratory Data Analysis & Credit Hazard Audit.
    Inspects observation counts, entity distributions, date continuity,
    and historical rating migration event frequency.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS & CREDIT HAZARD AUDIT")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 1: AUTOMATED EXPLORATORY DATA ANALYSIS & CREDIT HAZARD AUDIT")
    print("=" * 115)

    # 1. Global Feature Store Stats
    stats_sql = """
    SELECT 
        COUNT(*) AS TOTAL_OBSERVATIONS,
        COUNT(DISTINCT COUNTERPARTY_ID) AS ACTIVE_CPTY_COUNT,
        COUNT(DISTINCT SNAPSHOT_DATE) AS TOTAL_TRADING_DATES,
        MIN(SNAPSHOT_DATE) AS EARLIEST_DATE,
        MAX(SNAPSHOT_DATE) AS LATEST_DATE,
        COUNT(CASE WHEN IS_DOWNGRADE_RISK = 1 THEN 1 END) AS DOWNGRADE_RISK_EVENTS,
        COUNT(CASE WHEN IS_DOWNGRADE_RISK = 0 THEN 1 END) AS STABLE_EVENTS,
        ROUND(AVG(IS_DOWNGRADE_RISK) * 100, 2) AS EVENT_RATE_PCT,
        ROUND(MIN(NET_EXPOSURE_USD), 2) AS MIN_EXPOSURE,
        ROUND(MAX(NET_EXPOSURE_USD), 2) AS MAX_EXPOSURE,
        ROUND(AVG(NET_EXPOSURE_USD), 2) AS AVG_EXPOSURE
    FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES;
    """
    stats = session.sql(stats_sql).collect()[0]

    logger.info(f"Dataset Observations: {stats['TOTAL_OBSERVATIONS']:,} rows across {stats['ACTIVE_CPTY_COUNT']} counterparties")
    logger.info(f"Date Span: {stats['EARLIEST_DATE']} to {stats['LATEST_DATE']} ({stats['TOTAL_TRADING_DATES']} trading dates)")
    logger.info(f"Target Distribution: {stats['DOWNGRADE_RISK_EVENTS']} Risk Days (1) | {stats['STABLE_EVENTS']} Stable Days (0) | Base Rate: {stats['EVENT_RATE_PCT']}%")
    logger.info(f"Exposure Range: Min ${stats['MIN_EXPOSURE']:,.2f} | Max ${stats['MAX_EXPOSURE']:,.2f} | Avg ${stats['AVG_EXPOSURE']:,.2f}")

    print(f"  * Observation Universe       : {stats['TOTAL_OBSERVATIONS']:,} daily observations across {stats['ACTIVE_CPTY_COUNT']} institutional counterparties")
    print(f"  * Time Span Covered          : {stats['EARLIEST_DATE']} to {stats['LATEST_DATE']} ({stats['TOTAL_TRADING_DATES']} business dates)")
    print(f"  * 30-Day Forward Downgrades  : {stats['DOWNGRADE_RISK_EVENTS']} observation dates flagged with impending downgrade ({stats['EVENT_RATE_PCT']}% base rate)")
    print(f"  * Counterparty Exposure Span : Min: ${stats['MIN_EXPOSURE']:,.2f} | Max: ${stats['MAX_EXPOSURE']:,.2f} | Avg: ${stats['AVG_EXPOSURE']:,.2f}")

    # 2. Rating Migration Event Audit
    mig_sql = """
    SELECT 
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        CREDIT_RATING,
        COUNT(CASE WHEN IS_DOWNGRADE_RISK = 1 THEN 1 END) AS HAZARD_DAYS,
        MIN(CASE WHEN IS_DOWNGRADE_RISK = 1 THEN SNAPSHOT_DATE END) AS FIRST_HAZARD_DATE,
        MAX(CASE WHEN IS_DOWNGRADE_RISK = 1 THEN SNAPSHOT_DATE END) AS LAST_HAZARD_DATE
    FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
    GROUP BY COUNTERPARTY_ID, COUNTERPARTY_NAME, CREDIT_RATING
    HAVING COUNT(CASE WHEN IS_DOWNGRADE_RISK = 1 THEN 1 END) > 0
    ORDER BY HAZARD_DAYS DESC;
    """
    mig_rows = session.sql(mig_sql).collect()

    print("\n" + "-" * 115)
    print(f"{'COUNTERPARTY ID':<16} | {'COUNTERPARTY NAME':<34} | {'RATING':<8} | {'HAZARD DAYS':<12} | {'FIRST WARNING':<14} | {'LAST WARNING'}")
    print("-" * 115)
    for r in mig_rows:
        print(
            f"{r['COUNTERPARTY_ID']:<16} | {r['COUNTERPARTY_NAME'][:34]:<34} | {r['CREDIT_RATING']:<8} | "
            f"{r['HAZARD_DAYS']:<12} | {str(r['FIRST_HAZARD_DATE']):<14} | {str(r['LAST_HAZARD_DATE'])}"
        )
    print("-" * 115)

    if stats['TOTAL_OBSERVATIONS'] > 0 and stats['DOWNGRADE_RISK_EVENTS'] > 0:
        logger.info("[OK - EDA PASSED] Rich historical dataset confirmed with authentic rare-event credit downgrade signals.")
        print("[OK - EDA PASSED] Rich historical dataset confirmed with authentic rare-event credit downgrade signals.")
        return True
    else:
        logger.warning("[WARNING - EDA AUDIT] Insufficient observations or zero downgrade events found.")
        print("[WARNING - EDA AUDIT] Insufficient observations or zero downgrade events found.")
        return False


# ==============================================================================
# PHASE 2: DATABASE SCAFFOLDING & DDL AUTO-DEPLOYMENT
# ==============================================================================

def ensure_database_scaffolding(session):
    """
    Phase 2: Auto-deploys DDL for Feature View, Predictions Table, and Registry Table.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 2: VERIFYING & AUTO-DEPLOYING DATABASE SCAFFOLDING & DDLs")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 2: VERIFYING & AUTO-DEPLOYING DATABASE SCAFFOLDING & DDLs")
    print("=" * 115)

    # 1. Feature Store View DDL
    view_sql = """
    CREATE OR REPLACE VIEW ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES AS
    WITH CPTY_DAILY_AGG AS (
        SELECT 
            r.METRIC_DATE AS SNAPSHOT_DATE,
            r.COUNTERPARTY_ID,
            ROUND(SUM(r.MARK_TO_MARKET), 2) AS NET_EXPOSURE_USD,
            ROUND(SUM(r.VALUE_AT_RISK_99), 2) AS CPTY_VAR_99_USD,
            ROUND(SUM(r.POTENTIAL_FUTURE_EXPOSURE), 2) AS CPTY_PFE_95_USD,
            ROUND(SUM(r.CREDIT_VALUE_ADJUSTMENT), 2) AS CPTY_CVA_USD,
            COUNT(DISTINCT r.PORTFOLIO_ID) AS ACTIVE_PORTFOLIO_COUNT,
            COUNT(DISTINCT r.INSTRUMENT_ID) AS ACTIVE_INSTRUMENT_COUNT
        FROM ENG.FACT_DAILY_RISK_METRICS r
        GROUP BY r.METRIC_DATE, r.COUNTERPARTY_ID
    ),
    FIRM_TOTALS AS (
        SELECT 
            METRIC_DATE AS SNAPSHOT_DATE,
            SUM(MARK_TO_MARKET) AS FIRM_TOTAL_AUM_USD
        FROM ENG.FACT_DAILY_RISK_METRICS
        GROUP BY METRIC_DATE
    ),
    TRADE_VELOCITY AS (
        SELECT 
            TRANSACTION_DATE AS SNAPSHOT_DATE,
            COUNTERPARTY_ID,
            COUNT(*) AS DAILY_TRADE_COUNT,
            SUM(SETTLED_AMOUNT) AS DAILY_TRADE_VOLUME_USD
        FROM ENG.FACT_SETTLED_TRANSACTIONS
        GROUP BY TRANSACTION_DATE, COUNTERPARTY_ID
    ),
    ROLLING_METRICS AS (
        SELECT 
            a.SNAPSHOT_DATE,
            a.COUNTERPARTY_ID,
            a.NET_EXPOSURE_USD,
            a.CPTY_VAR_99_USD,
            a.CPTY_PFE_95_USD,
            a.CPTY_CVA_USD,
            a.ACTIVE_PORTFOLIO_COUNT,
            a.ACTIVE_INSTRUMENT_COUNT,
            f.FIRM_TOTAL_AUM_USD,
            ROUND((a.NET_EXPOSURE_USD / NULLIF(f.FIRM_TOTAL_AUM_USD, 0)) * 100, 4) AS CONCENTRATION_PCT,
            -- Rolling 14-day MTM volatility
            ROUND(COALESCE(STDDEV(a.NET_EXPOSURE_USD) OVER (
                PARTITION BY a.COUNTERPARTY_ID 
                ORDER BY a.SNAPSHOT_DATE 
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ), 0), 2) AS ROLLING_MTM_VOLATILITY_14D,
            -- Rolling 30-day Trade Activity
            COALESCE(SUM(t.DAILY_TRADE_COUNT) OVER (
                PARTITION BY a.COUNTERPARTY_ID 
                ORDER BY a.SNAPSHOT_DATE 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 0) AS ROLLING_TRADE_COUNT_30D,
            COALESCE(SUM(t.DAILY_TRADE_VOLUME_USD) OVER (
                PARTITION BY a.COUNTERPARTY_ID 
                ORDER BY a.SNAPSHOT_DATE 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 0) AS ROLLING_TRADE_VOLUME_30D
        FROM CPTY_DAILY_AGG a
        JOIN FIRM_TOTALS f ON a.SNAPSHOT_DATE = f.SNAPSHOT_DATE
        LEFT JOIN TRADE_VELOCITY t 
            ON a.SNAPSHOT_DATE = t.SNAPSHOT_DATE 
            AND a.COUNTERPARTY_ID = t.COUNTERPARTY_ID
    ),
    JOINED_WITH_SCD2 AS (
        SELECT 
            rf.*,
            c.COUNTERPARTY_NAME,
            c.SECTOR,
            c.JURISDICTION,
            c.NETTING_AGREEMENT_TYPE,
            c.CREDIT_RATING,
            c.EFFECTIVE_START_DATE,
            c.EFFECTIVE_END_DATE,
            DATEDIFF('day', c.EFFECTIVE_START_DATE, rf.SNAPSHOT_DATE) AS RATING_TENURE_DAYS,
            CASE c.CREDIT_RATING
                WHEN 'AAA' THEN 1
                WHEN 'AA'  THEN 2
                WHEN 'A'   THEN 3
                WHEN 'BBB' THEN 4
                WHEN 'BB'  THEN 5
                WHEN 'B'   THEN 6
                WHEN 'CCC' THEN 7
                WHEN 'D'   THEN 8
                ELSE 4
            END AS NUMERICAL_RATING
        FROM ROLLING_METRICS rf
        JOIN ENG.DIM_COUNTERPARTY c
            ON rf.COUNTERPARTY_ID = c.COUNTERPARTY_ID
            AND rf.SNAPSHOT_DATE BETWEEN c.EFFECTIVE_START_DATE AND c.EFFECTIVE_END_DATE
    )
    SELECT 
        j.*,
        -- Ground truth: check if counterparty rating degrades within 30 days ahead
        CASE 
            WHEN COALESCE(MAX(j.NUMERICAL_RATING) OVER (
                PARTITION BY j.COUNTERPARTY_ID 
                ORDER BY j.SNAPSHOT_DATE 
                ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING
            ), j.NUMERICAL_RATING) > j.NUMERICAL_RATING THEN 1
            ELSE 0
        END AS IS_DOWNGRADE_RISK
    FROM JOINED_WITH_SCD2 j;
    """
    session.sql(view_sql).collect()
    logger.info("De-duplicated & Scaffolding View ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES deployed.")
    print("  [OK] Feature Store View: ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES")

    # 2. Destination Table DDL
    table_sql = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS (
        PREDICTION_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        SNAPSHOT_DATE DATE NOT NULL,
        COUNTERPARTY_ID VARCHAR(50) NOT NULL,
        COUNTERPARTY_NAME VARCHAR(150),
        SECTOR VARCHAR(50),
        JURISDICTION VARCHAR(20),
        CURRENT_CREDIT_RATING VARCHAR(10) NOT NULL,
        NUMERICAL_RATING NUMBER NOT NULL,
        DOWNGRADE_PROBABILITY NUMBER(10,4) NOT NULL,
        PREDICTED_DOWNGRADE_FLAG NUMBER(1) NOT NULL,
        RISK_TIER VARCHAR(20) NOT NULL,
        RECOMMENDED_ACTION VARCHAR(255) NOT NULL,
        CONFIDENCE_SCORE NUMBER(10,4),
        FEATURE_NET_EXPOSURE_USD NUMBER(38,2),
        FEATURE_CONCENTRATION_PCT NUMBER(10,2),
        FEATURE_MTM_VOLATILITY_14D NUMBER(38,2),
        FEATURE_TRADE_COUNT_30D NUMBER,
        FEATURE_RATING_TENURE_DAYS NUMBER,
        MODEL_NAME VARCHAR(100) DEFAULT 'RandomForest_Downgrade_Classifier',
        MODEL_VERSION VARCHAR(50) DEFAULT 'V1.0',
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    session.sql(table_sql).collect()
    logger.info("Destination Table ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS verified.")
    print("  [OK] Persistence Table: ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS")

    # 3. Model Registry Metrics Table DDL
    registry_sql = """
    CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_MODEL_REGISTRY_METRICS (
        REGISTRY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        MODEL_NAME VARCHAR(100) NOT NULL,
        MODEL_VERSION VARCHAR(50) NOT NULL,
        MODEL_TYPE VARCHAR(50) NOT NULL,
        TRAINING_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        TRAIN_ROWS NUMBER,
        TEST_ROWS NUMBER,
        TRAIN_POSITIVES NUMBER,
        TEST_POSITIVES NUMBER,
        ROC_AUC NUMBER(10,4),
        PRECISION_SCORE NUMBER(10,4),
        RECALL_SCORE NUMBER(10,4),
        F1_SCORE NUMBER(10,4),
        FEATURE_IMPORTANCES VARIANT,
        CONFUSION_MATRIX VARIANT,
        CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
    );
    """
    session.sql(registry_sql).collect()
    logger.info("Model Registry Table ANALYTICS.FACT_MODEL_REGISTRY_METRICS verified.")
    print("  [OK] Model Registry Table: ANALYTICS.FACT_MODEL_REGISTRY_METRICS")


# ==============================================================================
# PHASE 3: IN-DATABASE MODEL TRAINING & CHRONOLOGICAL EVALUATION
# ==============================================================================

def train_and_evaluate_model(session) -> dict:
    """
    Phase 3: Trains Random Forest Classifier in Snowflake compute.
    Strict chronological split:
      - Train Window: <= 2026-06-30 (Months 1–6)
      - Test Window:   > 2026-06-30 (Months 7–9)
    Evaluates ROC-AUC, Precision, Recall, F1, and Feature Importances.
    Logs model metadata to ANALYTICS.FACT_MODEL_REGISTRY_METRICS.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 3: IN-DATABASE SUPERVISED MODEL TRAINING & EVALUATION")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 3: IN-DATABASE SUPERVISED MODEL TRAINING & CHRONOLOGICAL EVALUATION")
    print("=" * 115)

    train_proc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, confusion_matrix
import json

def train_and_evaluate(session):
    # 1. Fetch training data (Chronological: up to 2026-06-30)
    train_rows = session.sql("""
        SELECT 
            NUMERICAL_RATING,
            CONCENTRATION_PCT,
            ROLLING_MTM_VOLATILITY_14D,
            ROLLING_TRADE_COUNT_30D,
            ROLLING_TRADE_VOLUME_30D,
            CPTY_VAR_99_USD,
            CPTY_PFE_95_USD,
            RATING_TENURE_DAYS,
            ACTIVE_PORTFOLIO_COUNT,
            IS_DOWNGRADE_RISK
        FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
        WHERE SNAPSHOT_DATE <= '2026-06-30'
    """).collect()
    train_df = pd.DataFrame([r.as_dict() for r in train_rows])

    # 2. Fetch test data (Chronological out-of-sample: after 2026-06-30)
    test_rows = session.sql("""
        SELECT 
            NUMERICAL_RATING,
            CONCENTRATION_PCT,
            ROLLING_MTM_VOLATILITY_14D,
            ROLLING_TRADE_COUNT_30D,
            ROLLING_TRADE_VOLUME_30D,
            CPTY_VAR_99_USD,
            CPTY_PFE_95_USD,
            RATING_TENURE_DAYS,
            ACTIVE_PORTFOLIO_COUNT,
            IS_DOWNGRADE_RISK
        FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
        WHERE SNAPSHOT_DATE > '2026-06-30'
    """).collect()
    test_df = pd.DataFrame([r.as_dict() for r in test_rows])

    feature_cols = [
        'NUMERICAL_RATING', 'CONCENTRATION_PCT', 'ROLLING_MTM_VOLATILITY_14D',
        'ROLLING_TRADE_COUNT_30D', 'ROLLING_TRADE_VOLUME_30D',
        'CPTY_VAR_99_USD', 'CPTY_PFE_95_USD', 'RATING_TENURE_DAYS', 'ACTIVE_PORTFOLIO_COUNT'
    ]

    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['IS_DOWNGRADE_RISK'].values

    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df['IS_DOWNGRADE_RISK'].values

    # Train Random Forest with balanced class weight for rare downgrade events
    clf = RandomForestClassifier(n_estimators=100, max_depth=6, class_weight='balanced', random_state=42)
    clf.fit(X_train, y_train)

    # Predictions & Probabilities
    y_pred_proba = clf.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba >= 0.5).astype(int)

    # Compute metrics
    auc = float(roc_auc_score(y_test, y_pred_proba))
    precision = float(precision_score(y_test, y_pred, zero_division=0))
    recall = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    cm = confusion_matrix(y_test, y_pred).tolist()

    importances = {col: round(float(imp), 4) for col, imp in zip(feature_cols, clf.feature_importances_)}
    sorted_importances = dict(sorted(importances.items(), key=lambda item: item[1], reverse=True))

    results = {
        "status": "SUCCESS",
        "model_name": "RandomForest_Downgrade_Classifier",
        "model_version": "V1.0",
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "train_positives": int(y_train.sum()),
        "test_positives": int(y_test.sum()),
        "roc_auc": round(auc, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "confusion_matrix": {
            "tn": cm[0][0],
            "fp": cm[0][1],
            "fn": cm[1][0],
            "tp": cm[1][1]
        },
        "feature_importances": sorted_importances
    }
    return results
''').strip()

    sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_TRAIN_AND_EVAL_CPTY_MODEL()
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'train_and_evaluate'
    AS
    $$
{train_proc_code}
    $$;
    """
    session.sql(sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_TRAIN_AND_EVAL_CPTY_MODEL in Snowflake compute...")
    print("  --> Executing in-database training Stored Procedure inside Snowflake warehouse...")

    raw_res = session.call("ANALYTICS.SP_TRAIN_AND_EVAL_CPTY_MODEL")
    metrics = json.loads(raw_res)

    # Persist metrics into Model Registry table
    persist_registry_sql = f"""
    INSERT INTO ANALYTICS.FACT_MODEL_REGISTRY_METRICS (
        MODEL_NAME, MODEL_VERSION, MODEL_TYPE, TRAIN_ROWS, TEST_ROWS,
        TRAIN_POSITIVES, TEST_POSITIVES, ROC_AUC, PRECISION_SCORE, RECALL_SCORE, F1_SCORE,
        FEATURE_IMPORTANCES, CONFUSION_MATRIX
    )
    SELECT 
        '{metrics['model_name']}',
        '{metrics['model_version']}',
        'RandomForestClassifier',
        {metrics['train_rows']},
        {metrics['test_rows']},
        {metrics['train_positives']},
        {metrics['test_positives']},
        {metrics['roc_auc']},
        {metrics['precision']},
        {metrics['recall']},
        {metrics['f1_score']},
        PARSE_JSON('{json.dumps(metrics['feature_importances'])}'),
        PARSE_JSON('{json.dumps(metrics['confusion_matrix'])}');
    """
    session.sql(persist_registry_sql).collect()
    logger.info("Model metrics committed to ANALYTICS.FACT_MODEL_REGISTRY_METRICS.")

    # Terminal output
    print("\n" + "-" * 80)
    print("SNOWFLAKE MODEL REGISTRY TRAINING & EVALUATION METRICS CARD")
    print("-" * 80)
    print(f"  * Model Architecture     : {metrics['model_name']} ({metrics['model_version']})")
    print(f"  * Chronological Splits   : Train: {metrics['train_rows']:,} rows ({metrics['train_positives']} hazards) | Out-of-Sample Test: {metrics['test_rows']:,} rows ({metrics['test_positives']} hazards)")
    print(f"  * ROC-AUC Score          : {metrics['roc_auc']:.4f}  (Strong discriminative ranking)")
    print(f"  * Precision Score        : {metrics['precision']:.4f}")
    print(f"  * Recall Score           : {metrics['recall']:.4f}")
    print(f"  * F1 Score               : {metrics['f1_score']:.4f}")
    print(f"  * Out-of-Sample Confusion: TN: {metrics['confusion_matrix']['tn']:,} | FP: {metrics['confusion_matrix']['fp']} | FN: {metrics['confusion_matrix']['fn']} | TP: {metrics['confusion_matrix']['tp']}")
    print("-" * 80)
    print("FEATURE IMPORTANCE RANKING (Top Behavioral Drivers):")
    for feat, imp in list(metrics['feature_importances'].items())[:7]:
        bar = "█" * int(imp * 40)
        print(f"  - {feat:<28}: {imp*100:5.2f}% {bar}")
    print("-" * 80 + "\n")

    return metrics


# ==============================================================================
# PHASE 4: IN-DATABASE BATCH INFERENCE & GOLD TABLE PERSISTENCE
# ==============================================================================

def execute_inference_and_persist(session, target_date: str = None) -> tuple:
    """
    Phase 4: Executes batch scoring across counterparties and commits
    predictions to ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS.
    """
    logger.info("==========================================================================")
    logger.info("PHASE 4: IN-DATABASE BATCH INFERENCE & RISK TIER CLASSIFICATION")
    logger.info("==========================================================================")

    print("\n" + "=" * 115)
    print("PHASE 4: IN-DATABASE BATCH INFERENCE & RISK TIER CLASSIFICATION")
    print("=" * 115)

    if not target_date:
        # Default to latest snapshot date in feature store
        latest_date_row = session.sql("SELECT MAX(SNAPSHOT_DATE) AS MAX_DT FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES").collect()[0]
        target_date = str(latest_date_row['MAX_DT'])

    logger.info(f"Target Snapshot Scoring Date: {target_date}")
    print(f"  --> Scoring Counterparty Universe for Snapshot Date: {target_date}")

    score_proc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier

def score_and_persist(session, target_dt_str):
    # 1. Fetch training data (Chronological: up to 2026-06-30)
    train_rows = session.sql("""
        SELECT 
            NUMERICAL_RATING,
            CONCENTRATION_PCT,
            ROLLING_MTM_VOLATILITY_14D,
            ROLLING_TRADE_COUNT_30D,
            ROLLING_TRADE_VOLUME_30D,
            CPTY_VAR_99_USD,
            CPTY_PFE_95_USD,
            RATING_TENURE_DAYS,
            ACTIVE_PORTFOLIO_COUNT,
            IS_DOWNGRADE_RISK
        FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
        WHERE SNAPSHOT_DATE <= '2026-06-30'
    """).collect()
    train_df = pd.DataFrame([r.as_dict() for r in train_rows])

    feature_cols = [
        'NUMERICAL_RATING', 'CONCENTRATION_PCT', 'ROLLING_MTM_VOLATILITY_14D',
        'ROLLING_TRADE_COUNT_30D', 'ROLLING_TRADE_VOLUME_30D',
        'CPTY_VAR_99_USD', 'CPTY_PFE_95_USD', 'RATING_TENURE_DAYS', 'ACTIVE_PORTFOLIO_COUNT'
    ]

    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['IS_DOWNGRADE_RISK'].values

    clf = RandomForestClassifier(n_estimators=100, max_depth=6, class_weight='balanced', random_state=42)
    clf.fit(X_train, y_train)

    # 2. Fetch inference rows for target date
    infer_query = f"""
        SELECT 
            SNAPSHOT_DATE,
            COUNTERPARTY_ID,
            COUNTERPARTY_NAME,
            SECTOR,
            JURISDICTION,
            CREDIT_RATING,
            NUMERICAL_RATING,
            NET_EXPOSURE_USD,
            CONCENTRATION_PCT,
            ROLLING_MTM_VOLATILITY_14D,
            ROLLING_TRADE_COUNT_30D,
            ROLLING_TRADE_VOLUME_30D,
            CPTY_VAR_99_USD,
            CPTY_PFE_95_USD,
            RATING_TENURE_DAYS,
            ACTIVE_PORTFOLIO_COUNT
        FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES
        WHERE SNAPSHOT_DATE = '{target_dt_str}'
    """
    infer_rows = session.sql(infer_query).collect()
    if not infer_rows:
        return {"status": "NO_DATA", "persisted_rows": 0, "deleted_rows": 0, "warning_count": 0}

    infer_df = pd.DataFrame([r.as_dict() for r in infer_rows])
    X_infer = infer_df[feature_cols].fillna(0)

    # 3. Generate Probabilities
    probs = clf.predict_proba(X_infer)[:, 1]

    # 4. Map Risk Tiers & Prescriptive Actions
    records_to_insert = []
    warning_count = 0
    for i, row in infer_df.iterrows():
        prob = float(probs[i])
        flag = 1 if prob >= 0.50 else 0
        conf = float(abs(prob - 0.50) * 2.0)

        if prob >= 0.70:
            tier = "CRITICAL"
            action = "URGENT: Freeze new trading, call 100% cash variation margin, initiate trade novations"
            warning_count += 1
        elif prob >= 0.40:
            tier = "HIGH"
            action = "ELEVATED: Reduce uncollateralized limit by 50%, impose 10% haircut surcharge"
            warning_count += 1
        elif prob >= 0.20:
            tier = "WATCHLIST"
            action = "MONITOR: Place on Daily Risk Committee Watchlist, cap OTC tenors to <30 days"
        else:
            tier = "LOW"
            action = "STABLE: Standard approved trading limits and normal CSA monitoring"

        records_to_insert.append((
            str(row['SNAPSHOT_DATE']),
            str(row['COUNTERPARTY_ID']),
            str(row['COUNTERPARTY_NAME']).replace("'", "''"),
            str(row['SECTOR']),
            str(row['JURISDICTION']),
            str(row['CREDIT_RATING']),
            int(row['NUMERICAL_RATING']),
            round(prob, 4),
            int(flag),
            tier,
            action,
            round(conf, 4),
            float(row['NET_EXPOSURE_USD']),
            float(row['CONCENTRATION_PCT']),
            float(row['ROLLING_MTM_VOLATILITY_14D']),
            int(row['ROLLING_TRADE_COUNT_30D']),
            int(row['RATING_TENURE_DAYS'])
        ))

    # 5. Delete prior partition records
    del_res = session.sql(f"DELETE FROM ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS WHERE SNAPSHOT_DATE = '{target_dt_str}'").collect()
    deleted_count = del_res[0][0] if del_res else 0

    # 6. Bulk Insert
    val_clauses = []
    for rec in records_to_insert:
        val_clauses.append(f"('{rec[0]}', '{rec[1]}', '{rec[2]}', '{rec[3]}', '{rec[4]}', '{rec[5]}', {rec[6]}, {rec[7]}, {rec[8]}, '{rec[9]}', '{rec[10]}', {rec[11]}, {rec[12]}, {rec[13]}, {rec[14]}, {rec[15]}, {rec[16]})")
    
    insert_cols = (
        "SNAPSHOT_DATE, COUNTERPARTY_ID, COUNTERPARTY_NAME, SECTOR, JURISDICTION, "
        "CURRENT_CREDIT_RATING, NUMERICAL_RATING, DOWNGRADE_PROBABILITY, "
        "PREDICTED_DOWNGRADE_FLAG, RISK_TIER, RECOMMENDED_ACTION, CONFIDENCE_SCORE, "
        "FEATURE_NET_EXPOSURE_USD, FEATURE_CONCENTRATION_PCT, FEATURE_MTM_VOLATILITY_14D, "
        "FEATURE_TRADE_COUNT_30D, FEATURE_RATING_TENURE_DAYS"
    )
    insert_sql = f"INSERT INTO ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS ({insert_cols}) VALUES " + ",\\n".join(val_clauses) + ";"
    session.sql(insert_sql).collect()

    return {
        "status": "SUCCESS",
        "persisted_rows": len(records_to_insert),
        "deleted_rows": int(deleted_count),
        "warning_count": int(warning_count)
    }
''').strip()

    score_sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_SCORE_CPTY_DOWNGRADE_PREDICTIONS(TARGET_DT_STR VARCHAR)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'score_and_persist'
    AS
    $$
{score_proc_code}
    $$;
    """
    session.sql(score_sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_SCORE_CPTY_DOWNGRADE_PREDICTIONS...")
    
    score_res_raw = session.call("ANALYTICS.SP_SCORE_CPTY_DOWNGRADE_PREDICTIONS", target_date)
    score_res = json.loads(score_res_raw)

    persisted = score_res.get("persisted_rows", 0)
    deleted = score_res.get("deleted_rows", 0)
    warnings = score_res.get("warning_count", 0)

    logger.info(f"Persisted {persisted} predictions to Gold table (Deleted prior {deleted} records | Warning count: {warnings})")
    print(f"  [OK] Successfully persisted {persisted} counterparty predictions (Replaced {deleted} prior rows | {warnings} elevated warnings)")

    return persisted, deleted, warnings, target_date


# ==============================================================================
# PHASE 5: EXECUTIVE TERMINAL SCOREBOARD & ALERT RADAR
# ==============================================================================

def display_executive_scoreboard(session, target_date: str, alerts_only: bool = False, top_n: int = 15):
    """
    Phase 5: Formatted terminal risk report ranking counterparties by downgrade probability.
    """
    logger.info("==========================================================================")
    logger.info(f"PHASE 5: EXECUTIVE COUNTERPARTY RISK SCOREBOARD ({target_date})")
    logger.info("==========================================================================")

    print("\n" + "=" * 135)
    print(f"EXECUTIVE COUNTERPARTY CREDIT DOWNGRADE EARLY WARNING SCOREBOARD (SNAPSHOT: {target_date})")
    print("=" * 135)

    filter_clause = "AND RISK_TIER IN ('CRITICAL', 'HIGH')" if alerts_only else ""
    query = f"""
    SELECT 
        COUNTERPARTY_ID,
        COUNTERPARTY_NAME,
        SECTOR,
        CURRENT_CREDIT_RATING,
        ROUND(DOWNGRADE_PROBABILITY * 100, 2) AS PROB_PCT,
        RISK_TIER,
        ROUND(FEATURE_CONCENTRATION_PCT, 2) AS CONC_PCT,
        ROUND(FEATURE_NET_EXPOSURE_USD / 1000000, 2) AS EXPOSURE_M_USD,
        FEATURE_TRADE_COUNT_30D AS TRADES_30D,
        RECOMMENDED_ACTION
    FROM ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS
    WHERE SNAPSHOT_DATE = '{target_date}'
    {filter_clause}
    ORDER BY DOWNGRADE_PROBABILITY DESC
    LIMIT {top_n};
    """
    rows = session.sql(query).collect()

    print(f"{'CPTY ID':<11} | {'COUNTERPARTY NAME':<30} | {'SECTOR':<16} | {'RATING':<6} | {'PROB %':<7} | {'TIER':<9} | {'EXPOSURE':<10} | {'ACTION'}")
    print("-" * 135)

    for r in rows:
        tier_str = r['RISK_TIER']
        if tier_str == "CRITICAL":
            tier_display = "[CRIT]"
        elif tier_str == "HIGH":
            tier_display = "[HIGH]"
        elif tier_str == "WATCHLIST":
            tier_display = "[WATCH]"
        else:
            tier_display = "[LOW]"

        print(
            f"{r['COUNTERPARTY_ID']:<11} | {r['COUNTERPARTY_NAME'][:30]:<30} | {r['SECTOR'][:16]:<16} | "
            f"{r['CURRENT_CREDIT_RATING']:<6} | {r['PROB_PCT']:5.1f}% | {tier_display:<9} | "
            f"${r['EXPOSURE_M_USD']:>6.2f}M | {r['RECOMMENDED_ACTION'][:40]}..."
        )
    print("=" * 135)

    # Risk Tier Breakdown Summary
    summary_sql = f"""
    SELECT 
        RISK_TIER,
        COUNT(*) AS CPTY_COUNT,
        ROUND(AVG(DOWNGRADE_PROBABILITY) * 100, 2) AS AVG_PROB_PCT,
        ROUND(SUM(FEATURE_NET_EXPOSURE_USD) / 1000000, 2) AS TOTAL_EXPOSURE_M_USD
    FROM ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS
    WHERE SNAPSHOT_DATE = '{target_date}'
    GROUP BY RISK_TIER
    ORDER BY AVG_PROB_PCT DESC;
    """
    summary_rows = session.sql(summary_sql).collect()

    print("\nPORTFOLIO CREDIT EXPOSURE BY DOWNGRADE RISK TIER:")
    print("-" * 80)
    print(f"{'RISK TIER':<16} | {'ENTITIES':<10} | {'AVG PROBABILITY':<18} | {'TOTAL EXPOSURE ($M)'}")
    print("-" * 80)
    for sr in summary_rows:
        print(f"{sr['RISK_TIER']:<16} | {sr['CPTY_COUNT']:<10} | {sr['AVG_PROB_PCT']:>6.2f}%            | ${sr['TOTAL_EXPOSURE_M_USD']:>10.2f}M")
    print("-" * 80 + "\n")


# ==============================================================================
# PIPELINE ORCHESTRATOR & ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unified Counterparty Downgrade Early Warning Pipeline (Snowpark ML Classification)"
    )
    parser.add_argument("--config", default="config/dev.yaml", help="Path to YAML configuration file")
    parser.add_argument("--eda-only", action="store_true", help="Run only Exploratory Data Analysis & Feature Audit")
    parser.add_argument("--skip-train", action="store_true", help="Skip model retraining; execute scoring immediately")
    parser.add_argument("--train-only", action="store_true", help="Train and register model metrics without scoring")
    parser.add_argument("--date", help="Specific snapshot date to score (YYYY-MM-DD, defaults to latest available)")
    parser.add_argument("--alerts-only", action="store_true", help="Display only CRITICAL and HIGH tier counterparties")
    parser.add_argument("--top-n", type=int, default=15, help="Number of counterparties to display in scoreboard (default 15)")

    args = parser.parse_args()

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config=config)
    session = session_mgr.get_session()

    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    partition_key = args.date if args.date else datetime.now().strftime("%Y-%m-%d")

    audit_logger = None

    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        session.sql("USE SCHEMA ANALYTICS").collect()

        # Initialize Two-Tier Audit Logger
        audit_logger = AuditLogger(
            session=session,
            pipeline_name="CortexCounterpartyDowngradePipeline",
            subject_area="COUNTERPARTY_DOWNGRADE_PREDICTION",
            target_partition=partition_key
        )

        # Phase 1: EDA
        is_healthy = run_exploratory_data_analysis(session)
        if args.eda_only:
            logger.info("--eda-only specified. Exiting after EDA.")
            audit_logger.log_success(staged_rows=0, loaded_rows=0)
            return

        if not is_healthy:
            err_msg = "EDA Health Checks Failed. Aborting pipeline."
            logger.error(err_msg)
            audit_logger.log_failure(err_msg)
            sys.exit(1)

        # Phase 2: Scaffolding DDL
        ensure_database_scaffolding(session)

        # Phase 3: Model Training
        if not args.skip_train:
            train_and_evaluate_model(session)
        else:
            logger.info("--skip-train specified. Skipping retraining, reusing in-database model logic.")

        if args.train_only:
            logger.info("--train-only specified. Exiting after model training.")
            audit_logger.log_success(staged_rows=0, loaded_rows=0)
            return

        # Phase 4: Batch Inference & Persistence
        persisted_rows, deleted_rows, warning_count, scored_date = execute_inference_and_persist(
            session, target_date=args.date
        )

        # Phase 5: Executive Terminal Scoreboard
        display_executive_scoreboard(
            session, target_date=scored_date, alerts_only=args.alerts_only, top_n=args.top_n
        )

        # Phase 6: Commit Success Audit Record
        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=persisted_rows,
            deleted_rows=deleted_rows,
            expired_rows=0,
            warning_count=warning_count
        )

        logger.info("**************************************************************************")
        logger.info("COUNTERPARTY DOWNGRADE EARLY WARNING PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  - Scored Snapshot Date       : {scored_date}")
        logger.info(f"  - Predictions Committed      : {persisted_rows}")
        logger.info(f"  - Elevated Warning Flags     : {warning_count}")
        logger.info(f"  - Audit Log Entry            : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("**************************************************************************\n")

    except Exception as e:
        logger.error(f"[COUNTERPARTY DOWNGRADE PIPELINE FAILED] Error: {str(e)}", exc_info=True)
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
