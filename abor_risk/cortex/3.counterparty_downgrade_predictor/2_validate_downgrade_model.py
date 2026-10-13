"""
Stage 2: Model Validation & Decision Threshold Calibration
==========================================================
Module: abor_risk/cortex/3.counterparty_downgrade_predictor/2_validate_downgrade_model.py
Engine: Native Snowflake Compute & Snowpark In-Database ML
Target: Validates trained model on Validation Window (2026-07-01 to 2026-08-15)
Tunes:  Optimal probability classification threshold (F1-maximizing cutoff)
Updates: OPTIMAL_THRESHOLD in ANALYTICS.FACT_MODEL_STORE

Usage:
  python abor_risk/cortex/3.counterparty_downgrade_predictor/2_validate_downgrade_model.py
  python abor_risk/cortex/3.counterparty_downgrade_predictor/2_validate_downgrade_model.py --version V1.0
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

logger = get_logger("ValidateDowngradeModel", log_file="abor_ingestion.log")


def validate_model(session, model_version: str = "V1.0", start_date: str = "2026-07-01", end_date: str = "2026-08-15") -> dict:
    """
    Validates model artifact against the validation window (2026-07-01 to 2026-08-15).
    Determines optimal classification threshold and updates FACT_MODEL_STORE.
    """
    logger.info("==========================================================================")
    logger.info(f"STAGE 2: VALIDATING MODEL ({model_version}) ON WINDOW {start_date} TO {end_date}")
    logger.info("==========================================================================")

    print("\n" + "=" * 105)
    print(f"STAGE 2: MID-YEAR MODEL VALIDATION & THRESHOLD CALIBRATION ({start_date} TO {end_date})")
    print("=" * 105)

    sproc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, confusion_matrix
import pickle
import base64
import json

def validate_and_tune(session, model_version, start_date, end_date):
    # 1. Fetch trained model from store
    model_row = session.sql(f"""
        SELECT MODEL_PAYLOAD, FEATURE_LIST, OPTIMAL_THRESHOLD
        FROM ANALYTICS.FACT_MODEL_STORE
        WHERE MODEL_NAME = 'RandomForest_Downgrade_Classifier'
          AND MODEL_VERSION = '{model_version}'
        ORDER BY MODEL_ID DESC
        LIMIT 1
    """).collect()

    if not model_row:
        return {"status": "ERROR", "message": "Model not found in ANALYTICS.FACT_MODEL_STORE. Run 1_train_downgrade_model.py first."}

    model_b64 = model_row[0]['MODEL_PAYLOAD']
    feature_cols = json.loads(model_row[0]['FEATURE_LIST'])
    clf = pickle.loads(base64.b64decode(model_b64.encode('utf-8')))

    # 2. Fetch validation window records
    val_rows = session.sql(f"""
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

    val_df = pd.DataFrame([r.as_dict() for r in val_rows])
    if val_df.empty:
        return {"status": "ERROR", "message": "Validation window returned 0 records."}

    X_val = val_df[feature_cols].fillna(0)
    y_val = val_df['IS_DOWNGRADE_RISK'].values

    # 3. Generate probability predictions
    y_val_proba = clf.predict_proba(X_val)[:, 1]
    val_auc = float(roc_auc_score(y_val, y_val_proba))

    # 4. Threshold sweep across candidate cutoffs
    threshold_candidates = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
    sweep_results = []
    best_threshold = 0.50
    best_f1 = -1.0

    for th in threshold_candidates:
        preds = (y_val_proba >= th).astype(int)
        p = float(precision_score(y_val, preds, zero_division=0))
        r = float(recall_score(y_val, preds, zero_division=0))
        f = float(f1_score(y_val, preds, zero_division=0))
        cm = confusion_matrix(y_val, preds).tolist()

        sweep_results.append({
            "threshold": round(th, 2),
            "precision": round(p, 4),
            "recall": round(r, 4),
            "f1_score": round(f, 4),
            "tp": cm[1][1],
            "fp": cm[0][1],
            "fn": cm[1][0]
        })

        if f > best_f1:
            best_f1 = f
            best_threshold = th

    # 5. Evaluate at optimal threshold
    optimal_preds = (y_val_proba >= best_threshold).astype(int)
    opt_p = float(precision_score(y_val, optimal_preds, zero_division=0))
    opt_r = float(recall_score(y_val, optimal_preds, zero_division=0))
    opt_f = float(f1_score(y_val, optimal_preds, zero_division=0))
    opt_cm = confusion_matrix(y_val, optimal_preds).tolist()

    # 6. Update FACT_MODEL_STORE with tuned threshold
    session.sql(f"""
        UPDATE ANALYTICS.FACT_MODEL_STORE
        SET OPTIMAL_THRESHOLD = {best_threshold}
        WHERE MODEL_NAME = 'RandomForest_Downgrade_Classifier'
          AND MODEL_VERSION = '{model_version}';
    """).collect()

    return {
        "status": "SUCCESS",
        "model_version": str(model_version),
        "val_start": str(start_date),
        "val_end": str(end_date),
        "val_rows": len(val_df),
        "val_positives": int(y_val.sum()),
        "roc_auc": round(val_auc, 4),
        "optimal_threshold": round(best_threshold, 2),
        "precision": round(opt_p, 4),
        "recall": round(opt_r, 4),
        "f1_score": round(opt_f, 4),
        "confusion_matrix": {
            "tn": opt_cm[0][0],
            "fp": opt_cm[0][1],
            "fn": opt_cm[1][0],
            "tp": opt_cm[1][1]
        },
        "threshold_sweep": sweep_results
    }
''').strip()

    sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_VALIDATE_CPTY_DOWNGRADE_MODEL(MODEL_VER VARCHAR, START_DT VARCHAR, END_DT VARCHAR)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'validate_and_tune'
    AS
    $$
{sproc_code}
    $$;
    """
    session.sql(sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_VALIDATE_CPTY_DOWNGRADE_MODEL in Snowflake compute...")
    print("  --> Executing in-database validation & threshold calibration inside Snowflake warehouse...")

    raw_res = session.call("ANALYTICS.SP_VALIDATE_CPTY_DOWNGRADE_MODEL", model_version, start_date, end_date)
    results = json.loads(raw_res)

    if results.get("status") == "ERROR":
        raise RuntimeError(results.get("message"))

    print("\n" + "-" * 85)
    print("VALIDATION METRICS & THRESHOLD CALIBRATION CARD")
    print("-" * 85)
    print(f"  * Validation Window       : {results['val_start']} to {results['val_end']} (~30 business days)")
    print(f"  * Validation Observations : {results['val_rows']:,} rows ({results['val_positives']} downgrade hazard events)")
    print(f"  * Validation ROC-AUC      : {results['roc_auc']:.4f}")
    print(f"  * Calibrated Optimal Cutoff: P >= {results['optimal_threshold']:.2f}")
    print(f"  * Precision at Cutoff     : {results['precision']*100:.1f}%")
    print(f"  * Recall at Cutoff        : {results['recall']*100:.1f}%")
    print(f"  * F1 Score at Cutoff      : {results['f1_score']:.4f}")
    print(f"  * Confusion Matrix        : TN={results['confusion_matrix']['tn']:,} | FP={results['confusion_matrix']['fp']} | FN={results['confusion_matrix']['fn']} | TP={results['confusion_matrix']['tp']}")
    print("-" * 85)
    print("THRESHOLD SENSITIVITY SWEEP (Tradeoff between False Alarms & Missed Defaults):")
    print(f"  {'THRESHOLD':<10} | {'PRECISION':<12} | {'RECALL':<12} | {'F1-SCORE':<10} | {'TP':<4} | {'FP':<4} | {'STATUS'}")
    print("  " + "-" * 75)
    for sw in results['threshold_sweep']:
        is_opt = "★ [OPTIMAL]" if sw['threshold'] == results['optimal_threshold'] else ""
        print(f"  P >= {sw['threshold']:<5.2f}  | {sw['precision']*100:6.1f}%     | {sw['recall']*100:6.1f}%     | {sw['f1_score']:<10.4f} | {sw['tp']:<4} | {sw['fp']:<4} | {is_opt}")
    print("-" * 85 + "\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="Stage 2: Validate Counterparty Downgrade Classifier")
    parser.add_argument("--config", default="config/dev.yaml", help="Path to config file")
    parser.add_argument("--version", default="V1.0", help="Model version to validate")
    parser.add_argument("--start-date", default="2026-07-01", help="Validation start date")
    parser.add_argument("--end-date", default="2026-08-15", help="Validation end date")

    args = parser.parse_args()

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config=config)
    session = session_mgr.get_session()

    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")
    audit_logger = None

    try:
        session.sql(f"USE DATABASE {target_db}").collect()
        session.sql("USE SCHEMA ANALYTICS").collect()

        audit_logger = AuditLogger(
            session=session,
            pipeline_name="ValidateDowngradeModel",
            subject_area="MODEL_VALIDATION",
            target_partition=f"{args.version}_{args.start_date}"
        )

        res = validate_model(
            session=session,
            model_version=args.version,
            start_date=args.start_date,
            end_date=args.end_date
        )

        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=res['val_rows'],
            deleted_rows=0,
            expired_rows=0,
            warning_count=res['confusion_matrix']['fn']
        )
        logger.info("Validation completed successfully.")

    except Exception as e:
        logger.error(f"[MODEL VALIDATION FAILED] {str(e)}", exc_info=True)
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
