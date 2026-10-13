import sys
from pathlib import Path
import pytest
from unittest.mock import MagicMock
from snowflake.snowpark import Session

# Ensure root directory is always on python path
root_dir = str(Path(__file__).parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)


@pytest.fixture
def mock_snowpark_session():
    """Returns a mocked Snowpark Session for unit testing without live connection."""
    session = MagicMock(spec=Session)
    return session


@pytest.fixture
def sample_config():
    """Returns standard configuration dictionary fixture."""
    return {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "role": "TEST_ROLE",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "PUBLIC"
        },
        "pipeline": {
            "high_risk_threshold": 0.85,
            "log_level": "DEBUG"
        }
    }
