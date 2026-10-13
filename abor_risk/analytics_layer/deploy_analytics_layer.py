"""
Master Analytics Layer Deployer & Validator
============================================
Connects to Snowflake, ensures the ANALYTICS schema is provisioned, and deploys all
business data marts and reporting views:
  1. ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
  2. ANALYTICS.VW_COUNTERPARTY_CONCENTRATION
  3. ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION
  4. ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS
  5. ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION

Usage:
  python abor_risk/analytics_layer/deploy_analytics_layer.py --config config/dev.yaml
"""

import sys
import os
import argparse
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from abor_risk.utils.audit_logger import AuditLogger

from abor_risk.analytics_layer.views.create_executive_portfolio_risk_view import create_executive_portfolio_risk_view
from abor_risk.analytics_layer.views.create_counterparty_concentration_view import create_counterparty_concentration_view
from abor_risk.analytics_layer.views.create_asset_class_risk_view import create_asset_class_risk_view
from abor_risk.analytics_layer.views.create_trade_turnover_view import create_trade_turnover_view
from abor_risk.analytics_layer.views.create_rating_migration_view import create_rating_migration_view

logger = get_logger("AnalyticsLayerDeployer", log_file="abor_ingestion.log")


def deploy_all_analytics_views(session, target_db: str):
    """
    Executes DDL for all business views in the ANALYTICS schema.
    """
    logger.info("==========================================================================")
    logger.info(f"STARTING ANALYTICS LAYER DEPLOYMENT | Target DB: {target_db}")
    logger.info("==========================================================================")

    # 1. Ensure ANALYTICS Schema exists
    session.sql(f"USE DATABASE {target_db}").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS ANALYTICS").collect()
    session.sql("USE SCHEMA ANALYTICS").collect()

    audit = AuditLogger(session, "AnalyticsLayerDeployer", "ANALYTICS_VIEWS", "ALL")

    deployed_views = []
    try:
        # 2. Deploy individual views
        logger.info("1/5: Deploying VW_EXECUTIVE_PORTFOLIO_RISK...")
        create_executive_portfolio_risk_view(session)
        deployed_views.append("ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK")

        logger.info("2/5: Deploying VW_COUNTERPARTY_CONCENTRATION...")
        create_counterparty_concentration_view(session)
        deployed_views.append("ANALYTICS.VW_COUNTERPARTY_CONCENTRATION")

        logger.info("3/5: Deploying VW_ASSET_CLASS_RISK_DISTRIBUTION...")
        create_asset_class_risk_view(session)
        deployed_views.append("ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION")

        logger.info("4/5: Deploying VW_TRADE_TURNOVER_ANALYTICS...")
        create_trade_turnover_view(session)
        deployed_views.append("ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS")

        logger.info("5/5: Deploying VW_COUNTERPARTY_RATING_MIGRATION...")
        create_rating_migration_view(session)
        deployed_views.append("ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION")

        # 3. Validation: Verify each view can be queried
        logger.info("Validating deployed analytics views...")
        validation_results = {}
        for v in deployed_views:
            count = session.sql(f"SELECT COUNT(*) FROM {v}").collect()[0][0]
            validation_results[v] = count
            logger.info(f"  [VALIDATED] {v:<45} -> {count} accessible rows")

        audit.log_success(staged_rows=len(deployed_views), loaded_rows=len(deployed_views))

        print("\n" + "=" * 80)
        print("[SUCCESS] ANALYTICS CONSUMPTION LAYER DEPLOYMENT SUMMARY")
        print("=" * 80)
        for v, cnt in validation_results.items():
            print(f"  - {v:<46} : {cnt:>6} rows available")
        print("=" * 80 + "\n")

    except Exception as e:
        logger.error(f"[ANALYTICS DEPLOYMENT FAILED] Error: {str(e)}")
        audit.log_failure(str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description="Deploy Analytics Layer Data Marts and Views in Snowflake")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()

    config = load_config(args.config)
    target_db = config.get("snowflake", {}).get("database", "RISK_DEV_DB")

    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()

    try:
        deploy_all_analytics_views(session, target_db)
    finally:
        session.close()


if __name__ == "__main__":
    main()

