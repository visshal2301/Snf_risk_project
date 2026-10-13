import pytest
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_ACCOUNT"),
    reason="Snowflake environment variables not set. Skipping live integration test."
)
def test_live_snowflake_connection():
    """Integration test connecting to live Snowflake instance when env vars present."""
    from src.connection.session import SnowflakeSessionManager

    manager = SnowflakeSessionManager()
    session = manager.get_session()
    
    result = session.sql("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_ROLE()").collect()
    assert len(result) > 0
    session.close()
