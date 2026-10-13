"""
Streamlit in Snowflake (SiS) Deployment Script
==============================================
Deploys the Institutional Risk Command Center dashboard directly into Snowflake
as a native Streamlit in Snowflake (SiS) application.

Workflow:
  1. Creates internal stage ENG.STREAMLIT_STAGE if not exists.
  2. Uploads dashboard files (risk_dashboard.py, styles.py, data_service.py, contagion_engine.py).
  3. Creates or replaces STREAMLIT ANALYTICS.RISK_COMMAND_CENTER.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

load_dotenv(PROJECT_ROOT / ".env")

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager

logger = get_logger("DeploySiS", log_file="abor_ingestion.log")


def deploy_sis_app():
    cfg = load_config(PROJECT_ROOT / "config" / "dev.yaml")
    session = SnowflakeSessionManager(config=cfg).get_session()

    logger.info("Initializing Streamlit in Snowflake (SiS) deployment...")
    print("=" * 80)
    print("🏛️ DEPLOYING STREAMLIT IN SNOWFLAKE (SiS) RISK COMMAND CENTER")
    print("=" * 80)

    # 1. Create Internal Stage
    stage_sql = "CREATE STAGE IF NOT EXISTS ENG.STREAMLIT_STAGE DIRECTORY = (ENABLE = TRUE);"
    session.sql(stage_sql).collect()
    logger.info("Stage ENG.STREAMLIT_STAGE verified.")
    print("  [1/3] Internal Stage verified: ENG.STREAMLIT_STAGE")

    # 2. Upload Python modules to Snowflake stage
    streamlit_dir = Path(__file__).parent
    files_to_upload = [
        "risk_dashboard.py",
        "styles.py",
        "data_service.py",
        "contagion_engine.py"
    ]

    for fname in files_to_upload:
        fpath = streamlit_dir / fname
        if fpath.exists():
            put_res = session.file.put(
                local_file_name=str(fpath),
                stage_location="@ENG.STREAMLIT_STAGE",
                auto_compress=False,
                overwrite=True
            )
            print(f"  [2/3] Uploaded: {fname} -> @ENG.STREAMLIT_STAGE")
            logger.info(f"Uploaded {fname} to @ENG.STREAMLIT_STAGE: {put_res}")

    # 3. Create or Replace Streamlit in Snowflake Object
    sis_sql = """
    CREATE OR REPLACE STREAMLIT ANALYTICS.RISK_COMMAND_CENTER
    ROOT_LOCATION = '@ENG.STREAMLIT_STAGE'
    MAIN_FILE = 'risk_dashboard.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    COMMENT = 'Institutional ABOR & Quantitative Risk Command Center Dashboard';
    """
    session.sql(sis_sql).collect()
    logger.info("Streamlit in Snowflake application ANALYTICS.RISK_COMMAND_CENTER deployed.")
    print("  [3/3] Created Streamlit Object: ANALYTICS.RISK_COMMAND_CENTER")

    print("\n" + "=" * 80)
    print("✅ DEPLOYMENT COMPLETE! Open Snowflake Web UI -> Streamlit -> RISK_COMMAND_CENTER")
    print("=" * 80)

    session.close()


if __name__ == "__main__":
    deploy_sis_app()
