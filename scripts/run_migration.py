import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
load_dotenv()

from src.connection.session import SnowflakeSessionManager
from src.utils.logger import get_logger

logger = get_logger("SchemaMigration")


def run_migration(sql_file_path: str):
    logger.info("Initializing Snowflake session...")
    session_manager = SnowflakeSessionManager()
    session = session_manager.get_session()

    sql_path = Path(sql_file_path)
    if not sql_path.exists():
        logger.error(f"Migration file not found: {sql_file_path}")
        sys.exit(1)

    logger.info(f"Reading SQL migration script: {sql_file_path}")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Split SQL file into statements by semicolon
    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

    logger.info(f"Executing {len(statements)} SQL DDL statements...")
    for idx, stmt in enumerate(statements, 1):
        # Ignore comments-only statements
        lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
        clean_stmt = "\n".join(lines).strip()
        if not clean_stmt:
            continue
        
        logger.info(f"Executing Statement #{idx}...")
        logger.debug(clean_stmt)
        session.sql(clean_stmt).collect()

    logger.info("Schema migration executed successfully!")
    session.close()


if __name__ == "__main__":
    migration_file = "sql/migrations/V1.0.0__initial_schema.sql"
    run_migration(migration_file)
