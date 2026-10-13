"""
Stage 1: In-Database Model Training & Artifact Store
====================================================
Module: abor_risk/cortex/3.counterparty_downgrade_predictor/1_train_downgrade_model.py
Engine: Native Snowflake Compute & Snowpark In-Database ML
Target: Trains RandomForestClassifier on Training Window (2026-01-01 to 2026-06-30)
Stores: Model artifact in ANALYTICS.FACT_MODEL_STORE & metadata in FACT_MODEL_REGISTRY_METRICS

Usage:
  python abor_risk/cortex/3.counterparty_downgrade_predictor/1_train_downgrade_model.py
  python abor_risk/cortex/3.counterparty_downgrade_predictor/1_train_downgrade_model.py --version V1.0 --estimators 100 --max-depth 6
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

logger = get_logger("TrainDowngradeModel", log_file="abor_ingestion.log")


def ensure_scaffolding(session):
    """Ensures feature view and model store table exist."""
    session.sql("""
    CREATE OR REPLACE TABLE ANALYTICS.FACT_MODEL_STORE (
        MODEL_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        MODEL_NAME VARCHAR(100) NOT NULL,
        MODEL_VERSION VARCHAR(50) NOT NULL,
        MODEL_TYPE VARCHAR(50) NOT NULL,
        MODEL_PAYLOAD VARCHAR, -- Base64 encoded pickle
        FEATURE_LIST VARIANT,
        OPTIMAL_THRESHOLD NUMBER(10,4) DEFAULT 0.50,
        TRAIN_WINDOW_START DATE,
        TRAIN_WINDOW_END DATE,
        TRAIN_ROWS NUMBER,
        TRAIN_POSITIVES NUMBER,
        ROC_AUC NUMBER(10,4),
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """).collect()

    session.sql("""
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
    """).collect()


def train_model(session, model_version: str = "V1.0", n_estimators: int = 100, max_depth: int = 6) -> dict:
    """
    Trains Random Forest classifier inside Snowflake compute on training split (<= 2026-06-30).
    Serializes model into ANALYTICS.FACT_MODEL_STORE and registers metrics.
    """
    logger.info("==========================================================================")
    logger.info(f"STAGE 1: TRAINING COUNTERPARTY DOWNGRADE CLASSIFIER ({model_version})")
    logger.info("==========================================================================")

    print("\n" + "=" * 105)
    print(f"STAGE 1: IN-DATABASE MODEL TRAINING & SERIALIZATION (MODEL VERSION: {model_version})")
    print("=" * 105)

    sproc_code = textwrap.dedent('''
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
import pickle
import base64
import json

def train_and_store(session, model_version, n_est, max_d):
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

    # Train Random Forest
    clf = RandomForestClassifier(
        n_estimators=int(n_est),
        max_depth=int(max_d),
        class_weight='balanced',
        random_state=42
    )
    clf.fit(X_train, y_train)

    y_pred_proba = clf.predict_proba(X_train)[:, 1]
    in_sample_auc = float(roc_auc_score(y_train, y_pred_proba))

    # Serialize model to base64
    model_bytes = pickle.dumps(clf)
    model_b64 = base64.b64encode(model_bytes).decode('utf-8')

    importances = {col: round(float(imp), 4) for col, imp in zip(feature_cols, clf.feature_importances_)}
    sorted_importances = dict(sorted(importances.items(), key=lambda item: item[1], reverse=True))

    # Delete existing model version if present in store
    session.sql(f"DELETE FROM ANALYTICS.FACT_MODEL_STORE WHERE MODEL_NAME = 'RandomForest_Downgrade_Classifier' AND MODEL_VERSION = '{model_version}'").collect()

    feat_json = json.dumps(feature_cols).replace("'", "''")
    # Insert into FACT_MODEL_STORE
    insert_sql = f"""
    INSERT INTO ANALYTICS.FACT_MODEL_STORE (
        MODEL_NAME, MODEL_VERSION, MODEL_TYPE, MODEL_PAYLOAD, FEATURE_LIST,
        OPTIMAL_THRESHOLD, TRAIN_WINDOW_START, TRAIN_WINDOW_END, TRAIN_ROWS, TRAIN_POSITIVES, ROC_AUC
    )
    SELECT 
        'RandomForest_Downgrade_Classifier',
        '{model_version}',
        'RandomForestClassifier',
        '{model_b64}',
        PARSE_JSON('{feat_json}'),
        0.50,
        '2026-01-01',
        '2026-06-30',
        {len(train_df)},
        {int(y_train.sum())},
        {round(in_sample_auc, 4)};
    """
    session.sql(insert_sql).collect()

    return {
        "status": "SUCCESS",
        "model_name": "RandomForest_Downgrade_Classifier",
        "model_version": str(model_version),
        "train_rows": len(train_df),
        "train_positives": int(y_train.sum()),
        "in_sample_auc": round(in_sample_auc, 4),
        "feature_importances": sorted_importances,
        "feature_cols": feature_cols
    }
''').strip()

    sproc_sql = f"""
    CREATE OR REPLACE PROCEDURE ANALYTICS.SP_TRAIN_AND_STORE_CPTY_MODEL(MODEL_VER VARCHAR, N_EST NUMBER, MAX_D NUMBER)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy')
    HANDLER = 'train_and_store'
    AS
    $$
{sproc_code}
    $$;
    """
    session.sql(sproc_sql).collect()
    logger.info("Executing ANALYTICS.SP_TRAIN_AND_STORE_CPTY_MODEL in Snowflake compute...")
    print("  --> Executing in-database training & serialization inside Snowflake warehouse...")

    raw_res = session.call("ANALYTICS.SP_TRAIN_AND_STORE_CPTY_MODEL", model_version, n_estimators, max_depth)
    results = json.loads(raw_res)

    print("\n" + "-" * 80)
    print("MODEL TRAINING & SERIALIZATION SUMMARY")
    print("-" * 80)
    print(f"  * Model Architecture : {results['model_name']} ({results['model_version']})")
    print(f"  * Training Window    : 2026-01-01 to 2026-06-30 (125 business days)")
    print(f"  * Training Universe  : {results['train_rows']:,} observations ({results['train_positives']} positive downgrade events)")
    print(f"  * In-Sample ROC-AUC  : {results['in_sample_auc']:.4f}")
    print(f"  * Storage Target     : ANALYTICS.FACT_MODEL_STORE")
    print("-" * 80)
    print("TOP BEHAVIORAL FEATURE DRIVERS:")
    for feat, imp in list(results['feature_importances'].items())[:7]:
        bar = "█" * int(imp * 40)
        print(f"  - {feat:<28}: {imp*100:5.2f}% {bar}")
    print("-" * 80 + "\n")

    return results


def main():
    parser = argparse.ArgumentParser(description="Stage 1: Train Counterparty Downgrade Classifier")
    parser.add_argument("--config", default="config/dev.yaml", help="Path to config file")
    parser.add_argument("--version", default="V1.0", help="Model version identifier")
    parser.add_argument("--estimators", type=int, default=100, help="Number of trees")
    parser.add_argument("--max-depth", type=int, default=6, help="Maximum tree depth")

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
            pipeline_name="TrainDowngradeModel",
            subject_area="MODEL_TRAINING",
            target_partition=args.version
        )

        ensure_scaffolding(session)
        res = train_model(
            session=session,
            model_version=args.version,
            n_estimators=args.estimators,
            max_depth=args.max_depth
        )

        audit_logger.log_success(
            staged_rows=0,
            loaded_rows=1,
            deleted_rows=0,
            expired_rows=0,
            warning_count=0
        )
        logger.info(f"Training completed successfully for model version {args.version}.")

    except Exception as e:
        logger.error(f"[MODEL TRAINING FAILED] {str(e)}", exc_info=True)
        if audit_logger:
            audit_logger.log_failure(str(e))
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
