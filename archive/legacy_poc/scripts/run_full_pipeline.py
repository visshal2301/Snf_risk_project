import argparse
import sys
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from scripts.init_db import init_database
from src.pipelines.ingestion_pipeline import MarketRiskIngestionPipeline
from src.pipelines.market_risk_pipeline import MarketRiskAssessmentPipeline

logger = get_logger("FullPipeline")


def run_full_pipeline(config_path: str, as_of_date: str, data_dir: str = None):
    logger.info("==========================================================")
    logger.info("       STARTING FULL END-TO-END MARKET RISK PIPELINE       ")
    logger.info("==========================================================")
    
    # -------------------------------------------------------------------------
    # STEP 1: Database, Schema & Table DDL Setup
    # -------------------------------------------------------------------------
    logger.info(">>> STEP 1: INITIALIZING DATABASE, SCHEMAS & TABLES...")
    init_database(config_path)

    # -------------------------------------------------------------------------
    # Connect with Standard Snowflake Session
    # -------------------------------------------------------------------------
    config = load_config(config_path)
    session_manager = SnowflakeSessionManager(config)
    session = session_manager.get_session()

    try:
        # -------------------------------------------------------------------------
        # STEP 2: Ingestion Layer (Pandas -> Snowpark DF -> RAW Schema)
        # -------------------------------------------------------------------------
        logger.info(">>> STEP 2: EXECUTING INGESTION LAYER (CSV -> RAW.RAW_POSITIONS & RAW.RAW_FACTOR_RETURNS)...")
        ingestion_pipeline = MarketRiskIngestionPipeline(
            session=session,
            config=config,
            data_dir=data_dir,
        )
        ingestion_pipeline.run()

        # -------------------------------------------------------------------------
        # STEP 3: Analytics & Transformation Layer (RAW -> ANALYTICS Schema)
        # -------------------------------------------------------------------------
        logger.info(f">>> STEP 3: EXECUTING MARKET RISK ANALYTICS LAYER FOR AS_OF_DATE='{as_of_date}'...")
        market_risk_pipeline = MarketRiskAssessmentPipeline(
            session=session,
            config=config,
            as_of_date=as_of_date,
        )
        market_risk_pipeline.run()

        # -------------------------------------------------------------------------
        # STEP 4: Results Verification & Summary Report
        # -------------------------------------------------------------------------
        logger.info(">>> STEP 4: FETCHING LATEST RISK METRICS FROM ANALYTICS.RISK_METRICS...")
        run_id = market_risk_pipeline.run_id
        metrics_df = (
            session.table("ANALYTICS.RISK_METRICS")
            .filter(f"RUN_ID = '{run_id}'")
            .to_pandas()
        )

        logger.info("==========================================================")
        logger.info(f" PIPELINE COMPLETED SUCCESSFULLY! RUN_ID: {run_id}")
        logger.info("==========================================================")
        if not metrics_df.empty:
            for idx, row in metrics_df.iterrows():
                logger.info(
                    f"Portfolio: {row['PORTFOLIO']} | AsOf: {row['AS_OF_DATE']} | "
                    f"VaR (99%): ${row['VAR_USD']:,.2f} | ES (99%): ${row['ES_USD']:,.2f}"
                )
        logger.info("==========================================================")

    except Exception as e:
        logger.error(f"Full pipeline execution failed: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Run Full Market Risk End-to-End Pipeline.")
    parser.add_argument("--config", type=str, default="config/dev.yaml", help="Path to YAML config file (e.g. config/dev.yaml or config/prod.yaml)")
    parser.add_argument("--as-of-date", type=str, default="2026-09-25", help="Portfolio snapshot date (YYYY-MM-DD)")
    parser.add_argument("--data-dir", type=str, default=None, help="Custom directory containing CSV files")
    args = parser.parse_args()

    run_full_pipeline(
        config_path=args.config,
        as_of_date=args.as_of_date,
        data_dir=args.data_dir,
    )


if __name__ == "__main__":
    main()
