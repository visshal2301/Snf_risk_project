import os
from typing import Any, Dict, Optional
from snowflake.snowpark import Session
from src.utils.logger import get_logger

logger = get_logger("SnowflakeSessionManager")


class SnowflakeSessionManager:
    """Production manager for creating and configuring Snowflake Snowpark sessions."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    def get_session(self, bootstrap: bool = False) -> Session:
        """Builds and returns an active Snowpark Session.
        
        Args:
            bootstrap (bool): If True, connects with admin/fallback parameters from .env
                              without requiring target database/role to exist yet.
        """
        sf_config = self.config.get("snowflake", {})
        
        if bootstrap:
            connection_parameters = {
                "account": sf_config.get("account") or os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": sf_config.get("user") or os.getenv("SNOWFLAKE_USER"),
                "password": sf_config.get("password") or os.getenv("SNOWFLAKE_PASSWORD"),
                "role": os.getenv("SNOWFLAKE_ROLE") or sf_config.get("role"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE") or sf_config.get("warehouse"),
            }
        else:
            # Build connection parameters from config or environment fallback
            connection_parameters = {
                "account": sf_config.get("account") or os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": sf_config.get("user") or os.getenv("SNOWFLAKE_USER"),
                "password": sf_config.get("password") or os.getenv("SNOWFLAKE_PASSWORD"),
                "role": sf_config.get("role") or os.getenv("SNOWFLAKE_ROLE"),
                "warehouse": sf_config.get("warehouse") or os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": sf_config.get("database") or os.getenv("SNOWFLAKE_DATABASE"),
                "schema": sf_config.get("schema") or os.getenv("SNOWFLAKE_SCHEMA"),
            }

        # RSA Key Authentication Support
        private_key_path = sf_config.get("private_key_path") or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if private_key_path and os.path.exists(private_key_path):
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            passphrase = sf_config.get("private_key_passphrase") or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            pass_bytes = passphrase.encode() if passphrase else None

            with open(private_key_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=pass_bytes,
                    backend=default_backend()
                )
            
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            connection_parameters["private_key"] = pkb
            # Password not needed if using RSA key
            connection_parameters.pop("password", None)

        # Remove None values
        connection_parameters = {k: v for k, v in connection_parameters.items() if v is not None}

        logger.info(f"Connecting to Snowflake account '{connection_parameters.get('account')}' (bootstrap={bootstrap})...")
        session = Session.builder.configs(connection_parameters).create()
        logger.info("Snowpark Session successfully created.")
        return session
