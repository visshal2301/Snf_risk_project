import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env file automatically
load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager

logger = get_logger("InitDB")


def init_database(config_path: str):
    logger.info(f"Loading environment configuration from: {config_path}")
    config = load_config(config_path)

    sf_config = config.get("snowflake", {})
    database_name = sf_config.get("database")
    warehouse_name = sf_config.get("warehouse")
    role_name = sf_config.get("role")
    schema_name = sf_config.get("schema", "PUBLIC")

    if not database_name:
        raise ValueError("Database name is missing in YAML configuration!")

    logger.info(f"Target Database: {database_name}")
    logger.info(f"Target Warehouse: {warehouse_name}")
    logger.info(f"Target Role: {role_name}")
    logger.info(f"Target Schema: {schema_name}")

    # Step 1: Connect using bootstrap mode (admin fallback) to create warehouse, database, schema
    session_manager = SnowflakeSessionManager(config)
    session = session_manager.get_session(bootstrap=True)

    try:
        if warehouse_name:
            logger.info(f"Ensuring Warehouse '{warehouse_name}' exists...")
            session.sql(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name} WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE").collect()
            session.sql(f"USE WAREHOUSE {warehouse_name}").collect()

        logger.info(f"Ensuring Database '{database_name}' exists...")
        session.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}").collect()

        logger.info(f"Switching context to Database '{database_name}'...")
        session.sql(f"USE DATABASE {database_name}").collect()

        logger.info(f"Ensuring Schema '{schema_name}' exists...")
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
        session.sql(f"USE SCHEMA {schema_name}").collect()

        # Step 2: Execute Migration Scripts
        migrations_dir = Path("sql/migrations")
        if migrations_dir.exists():
            migration_files = sorted(migrations_dir.glob("*.sql"))
            for migration_file in migration_files:
                logger.info(f"Applying migration SQL from {migration_file.name}...")
                sql_script = migration_file.read_text(encoding="utf-8")
                
                # Split commands by semicolon and run each statement
                raw_statements = [s.strip() for s in sql_script.split(";") if s.strip()]
                for raw_stmt in raw_statements:
                    cleaned_lines = [line for line in raw_stmt.splitlines() if not line.strip().startswith("--")]
                    stmt = "\n".join(cleaned_lines).strip()
                    if stmt:
                        logger.info(f"Executing statement: {stmt[:60]}...")
                        session.sql(stmt).collect()

        logger.info(f"Database '{database_name}' initialized successfully!")

    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        sys.exit(1)
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Dynamically create & initialize Snowflake database from YAML config.")
    parser.add_argument("--config", type=str, default="config/dev.yaml", help="Path to YAML config file (e.g. config/dev.yaml or config/prod.yaml)")
    args = parser.parse_args()

    init_database(args.config)


if __name__ == "__main__":
    main()
