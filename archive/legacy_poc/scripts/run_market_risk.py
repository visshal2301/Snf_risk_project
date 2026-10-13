import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from src.pipelines.market_risk_pipeline import MarketRiskAssessmentPipeline

logger = get_logger("RunMarketRisk")


def main():
    parser = argparse.ArgumentParser(description="Run Market Risk Historical Simulation ETL Pipeline.")
    parser.add_argument("--config", type=str, default="config/dev.yaml", help="Path to YAML config file")
    parser.add_argument("--as-of-date", type=str, default="2026-09-25", help="Portfolio snapshot date (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info(f"Loading environment configuration from: {args.config}")
    config = load_config(args.config)

    session_manager = SnowflakeSessionManager(config)
    session = session_manager.get_session()

    try:
        pipeline = MarketRiskAssessmentPipeline(
            session=session,
            config=config,
            as_of_date=args.as_of_date,
        )
        logger.info(f"Executing Market Risk Assessment Pipeline for AS_OF_DATE='{args.as_of_date}'...")
        pipeline.run()
        logger.info("Market Risk Assessment Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Market Risk Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
