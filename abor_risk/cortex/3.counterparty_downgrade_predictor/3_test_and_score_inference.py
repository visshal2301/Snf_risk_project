"""
Stage 3: Out-of-Sample Testing & Forward Daily Inference Scoring
================================================================
Module: abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py
Engine: Native Snowflake Compute & Snowpark In-Database ML
Targets:
  1. Out-of-Sample Holdout Testing (2026-08-16 to 2026-09-30)
  2. Forward Scoring for Target Snapshot Date & Gold Table Persistence (FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS)

Usage:
  # 1. Forward Snapshot Scoring on Latest Available Date
  python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py

  # 2. Run Holdout Out-of-Sample Test Evaluation
  python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py --eval-holdout

  # 3. Score a Specific Snapshot Date
  python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py --date 2026-09-30

  # 4. Filter for Critical & High Risk Alerts Only
  python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py --alerts-only
"""

import sys
import os
import json
import textwrap
import argparse
from datetime import datetime
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

logger = get_logger("TestAndScoreInference", log_file="abor_ingestion.log")


def run_holdout_test_evaluation(session, model_version: str = "V1.0", start_date: str = "2026-08-16", end_date: str = "2026-09-30") -> dict:
    """
    Evaluates final holdout test window (2026-08-16 to 2026-09-30) using calibrated threshold.
    """
    logger.info("==========================================================================")
    logger.info(f"STAGE 3A: EVALUATING OUT-OF-SAMPLE TEST HOLDOUT ({start_date} TO {end_date})")
    logger.info("==========================================================================")

    print("\n" + "=" * 105)
    print(f"STAGE 3A: FINAL OUT-OF-SAMPLE HOLDOUT TEST EVALUATION ({start_date} TO {end_date})")
    print("=" * 105)

    sproc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, confusion_matrix
import pickle
import base64
import json

def test_holdout(session, model_version, start_date, end_date):
    # 1. Fetch trained model and tuned threshold from store
    model_row = session.sql(f"""
        SELECT MODEL_PAYLOAD, FEATURE_LIST, OPTIMAL_THRESHOLD
        FROM ANALYTICS.FACT_MODEL_STORE
        WHERE MODEL_NAME = 'RandomForest_Downgrade_Classifier'
          AND MODEL_VERSION = '{model_version}'
        ORDER BY MODEL_ID DESC
        LIMIT 1
    """).collect()

    if not model_row:
        return {"status": "ERROR", "message": "Model not found. Run 1_train_downgrade_model.py first."}

    model_b64 = model_row[0]['MODEL_PAYLOAD']
    feature_cols = json.loads(model_row[0]['FEATURE_LIST'])
    threshold = float(model_row[0]['OPTIMAL_THRESHOLD'] or 0.50)
    clf = pickle.loads(base64.b64decode(model_b64.encode('utf-8')))

    # 2. Fetch test window records
    test_rows = session.sql(f"""
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
        WHERE SNAPSHOT_DATE BETWEEN '{start_date}' AND '{end_date}'
    """).collect()

    test_df = pd.DataFrame([r.as_dict() for r in test_rows])
    if test_df.empty:
        return {"status": "ERROR", "message": "Test holdout returned 0 records."}

    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df['IS_DOWNGRADE_RISK'].values

    y_test_proba = clf.predict_proba(X_test)[:, 1]
    test_auc = float(roc_auc_score(y_test, y_test_proba))

    preds = (y_test_proba >= threshold).astype(int)
    p = float(precision_score(y_test, preds, zero_division=0))
    r = float(recall_score(y_test, preds, zero_division=0))
    f = float(f1_score(y_test, preds, zero_division=0))
    cm = confusion_matrix(y_test, preds).tolist()

    return {
        "status": "SUCCESS",
        "model_version": str(model_version),
        "test_start": str(start_date),
        "test_end": str(end_date),
        "test_rows": len(test_df),
        "test_positives": int(y_test.sum()),
        "threshold_used": round(threshold, 2),
        "roc_auc": round(test_auc, 4),
        "precision": round(p, 4),
        "recall": round(r, 4),
        "f1_score": round(f, 4),
        "confusion_matrix": {
            "tn": cm[0][0],
            "fp": cm[0][1],
            "fn": cm[1][0],
            "tp": cm[1][1]
        }
    }
''').strip()

    sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_TEST_HOLDOUT_CPTY_MODEL(MODEL_VER VARCHAR, START_DT VARCHAR, END_DT VARCHAR)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'test_holdout'
    AS
    $$
{sproc_code}
    $$;
    """
    session.sql(sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_TEST_HOLDOUT_CPTY_MODEL in Snowflake compute...")
    print("  --> Executing out-of-sample holdout evaluation inside Snowflake warehouse...")

    raw_res = session.call("ANALYTICS.SP_TEST_HOLDOUT_CPTY_MODEL", model_version, start_date, end_date)
    results = json.loads(raw_res)

    if results.get("status") == "ERROR":
        raise RuntimeError(results.get("message"))

    print("\n" + "-" * 85)
    print("FINAL OUT-OF-SAMPLE HOLDOUT TEST SCORECARD")
    print("-" * 85)
    print(f"  * Holdout Test Window   : {results['test_start']} to {results['test_end']} (~35 business days)")
    print(f"  * Test Observations     : {results['test_rows']:,} rows ({results['test_positives']} downgrade hazard events)")
    print(f"  * Operating Threshold   : P >= {results['threshold_used']:.2f} (from Stage 2 Calibration)")
    print(f"  * Holdout ROC-AUC       : {results['roc_auc']:.4f}")
    print(f"  * Holdout Precision     : {results['precision']*100:.1f}%")
    print(f"  * Holdout Recall        : {results['recall']*100:.1f}%")
    print(f"  * Holdout F1-Score      : {results['f1_score']:.4f}")
    print(f"  * Confusion Matrix      : TN={results['confusion_matrix']['tn']:,} | FP={results['confusion_matrix']['fp']} | FN={results['confusion_matrix']['fn']} | TP={results['confusion_matrix']['tp']}")
    print("-" * 85 + "\n")

    return results


def score_snapshot_date(session, model_version: str = "V1.0", target_date: str = None) -> tuple:
    """
    Scores counterparty universe on specified snapshot date and commits
    predictions to ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS.
    """
    logger.info("==========================================================================")
    logger.info("STAGE 3B: BATCH INFERENCE & GOLD TABLE PERSISTENCE")
    logger.info("==========================================================================")

    print("\n" + "=" * 105)
    print("STAGE 3B: BATCH INFERENCE SCORING & GOLD TABLE PERSISTENCE")
    print("=" * 105)

    if not target_date:
        latest_date_row = session.sql("SELECT MAX(SNAPSHOT_DATE) AS MAX_DT FROM ANALYTICS.VW_ML_CPTY_DOWNGRADE_FEATURES").collect()[0]
        target_date = str(latest_date_row['MAX_DT'])

    logger.info(f"Target Snapshot Scoring Date: {target_date}")
    print(f"  --> Scoring Counterparty Universe for Snapshot Date: {target_date}")

    sproc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
import pickle
import base64
import json

def score_date(session, model_version, target_dt_str):
    # 1. Fetch trained model
    model_row = session.sql(f"""
        SELECT MODEL_PAYLOAD, FEATURE_LIST
        FROM ANALYTICS.FACT_MODEL_STORE
        WHERE MODEL_NAME = 'RandomForest_Downgrade_Classifier'
          AND MODEL_VERSION = '{model_version}'
        ORDER BY MODEL_ID DESC
        LIMIT 1
    """).collect()

    if not model_row:
        return {"status": "ERROR", "message": "Model not found in FACT_MODEL_STORE."}

    model_b64 = model_row[0]['MODEL_PAYLOAD']
    feature_cols = json.loads(model_row[0]['FEATURE_LIST'])
    clf = pickle.loads(base64.b64decode(model_b64.encode('utf-8')))

    # 2. Fetch inference rows
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

    # 3. Predict Probabilities
    probs = clf.predict_proba(X_infer)[:, 1]

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

    # 4. Clean prior partition & insert
    del_res = session.sql(f"DELETE FROM ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS WHERE SNAPSHOT_DATE = '{target_dt_str}'").collect()
    deleted_count = del_res[0][0] if del_res else 0

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

    sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_SCORE_SNAPSHOT_CPTY_MODEL(MODEL_VER VARCHAR, TARGET_DT_STR VARCHAR)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'score_date'
    AS
    $$
{sproc_code}
    $$;
    """
    session.sql(sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_SCORE_SNAPSHOT_CPTY_MODEL in Snowflake compute...")

    raw_res = session.call("ANALYTICS.SP_SCORE_SNAPSHOT_CPTY_MODEL", model_version, target_date)
    score_res = json.loads(raw_res)

    if score_res.get("status") == "ERROR":
        raise RuntimeError(score_res.get("message"))

    persisted = score_res.get("persisted_rows", 0)
    deleted = score_res.get("deleted_rows", 0)
    warnings = score_res.get("warning_count", 0)

    logger.info(f"Persisted {persisted} predictions to Gold table (Deleted prior {deleted} records | Warning count: {warnings})")
    print(f"  [OK] Successfully persisted {persisted} counterparty predictions (Replaced {deleted} prior rows | {warnings} elevated warnings)")

    return persisted, deleted, warnings, target_date


def display_scoreboard(session, target_date: str, alerts_only: bool = False, top_n: int = 15):
    """Prints terminal scoreboard ranking counterparties by risk."""
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


def main():
    parser = argparse.ArgumentParser(description="Stage 3: Out-of-Sample Test & Batch Scoring")
    parser.add_argument("--config", default="config/dev.yaml", help="Path to config file")
    parser.add_argument("--version", default="V1.0", help="Model version to score with")
    parser.add_argument("--eval-holdout", action="store_true", help="Execute out-of-sample holdout test evaluation")
    parser.add_argument("--date", help="Specific snapshot date to score (default latest available)")
    parser.add_argument("--alerts-only", action="store_true", help="Show only CRITICAL and HIGH tier alerts")
    parser.add_argument("--top-n", type=int, default=15, help="Top N entities in terminal scoreboard")

    args = parser.parse_args()

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config=config)
    session = session_mgr.get_session()

    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    audit_logger = None

    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        session.sql("USE SCHEMA ANALYTICS").collect()

        partition_key = args.date if args.date else datetime.now().strftime("%Y-%m-%d")

        audit_logger = AuditLogger(
            session=session,
            pipeline_name="TestAndScoreInference",
            subject_area="INFERENCE_SCORING",
            target_partition=partition_key
        )

        if args.eval_holdout:
            run_holdout_test_evaluation(session, model_version=args.version)

        persisted, deleted, warnings, scored_dt = score_snapshot_date(
            session, model_version=args.version, target_date=args.date
        )

        display_scoreboard(
            session, target_date=scored_dt, alerts_only=args.alerts_only, top_n=args.top_n
        )

        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=persisted,
            deleted_rows=deleted,
            expired_rows=0,
            warning_count=warnings
        )
        logger.info("Stage 3 completed successfully.")

    except Exception as e:
        logger.error(f"[INFERENCE SCORING FAILED] {str(e)}", exc_info=True)
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
