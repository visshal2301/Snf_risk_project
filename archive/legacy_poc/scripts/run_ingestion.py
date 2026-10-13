import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from src.pipelines.ingestion_pipeline import MarketRiskIngestionPipeline

logger = get_logger("RunIngestion")


def main():
    parser = argparse.ArgumentParser(description="Run Market Risk CSV Data Ingestion Pipeline.")
    parser.add_argument("--config", type=str, default="config/dev.yaml", help="Path to YAML config file")
    parser.add_argument("--data-dir", type=str, default=None, help="Directory containing positions.csv and factor_returns.csv")
    args = parser.parse_args()

    logger.info(f"Loading environment configuration from: {args.config}")
    config = load_config(args.config)

    session_manager = SnowflakeSessionManager(config)
    session = session_manager.get_session()

    try:
        pipeline = MarketRiskIngestionPipeline(
            session=session,
            config=config,
            data_dir=args.data_dir,
        )
        logger.info("Executing Market Risk Data Ingestion Pipeline...")
        pipeline.run()
        logger.info("Ingestion Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Ingestion Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
