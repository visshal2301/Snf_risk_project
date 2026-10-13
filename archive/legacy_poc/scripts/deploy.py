import sys
import argparse
from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from src.udfs.risk_udfs import register_udfs
from src.sprocs.risk_sprocs import register_sprocs

logger = get_logger("DeployScript")


def main():
    parser = argparse.ArgumentParser(description="Deploy Snowpark UDFs and Stored Procedures.")
    parser.add_argument("--config", type=str, default="config/dev.yaml", help="Path to YAML config file")
    args = parser.parse_args()

    logger.info(f"Loading environment configuration from: {args.config}")
    config = load_config(args.config)

    session_manager = SnowflakeSessionManager(config)
    session = session_manager.get_session()

    try:
        logger.info("Registering UDFs...")
        register_udfs(session)

        logger.info("Registering Stored Procedures...")
        register_sprocs(session)

        logger.info("Deployment completed successfully!")
    except Exception as e:
        logger.error(f"Deployment failed with error: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
