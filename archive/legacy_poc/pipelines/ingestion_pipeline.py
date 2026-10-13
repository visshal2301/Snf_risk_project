import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import pandas as pd
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, current_timestamp, to_date
from src.pipelines.base_pipeline import BasePipeline


class MarketRiskIngestionPipeline(BasePipeline):
    """Production Data Ingestion Pipeline loading CSV files via Pandas into RAW schema tables."""

    DEFAULT_DATA_DIR = Path("data")

    def __init__(
        self,
        session: Session,
        config: Dict[str, Any],
        data_dir: Optional[str] = None,
    ):
        super().__init__(session, config)
        self.data_dir = Path(data_dir) if data_dir else self.DEFAULT_DATA_DIR

    def extract(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Read positions.csv and factor_returns.csv using Pandas."""
        positions_path = self.data_dir / "positions.csv"
        returns_path = self.data_dir / "factor_returns.csv"

        if not positions_path.exists():
            raise FileNotFoundError(f"Positions file not found at: {positions_path}")
        if not returns_path.exists():
            raise FileNotFoundError(f"Factor returns file not found at: {returns_path}")

        self.logger.info(f"Extracting Pandas DataFrame from: {positions_path}")
        positions_pd = pd.read_csv(positions_path)

        self.logger.info(f"Extracting Pandas DataFrame from: {returns_path}")
        returns_pd = pd.read_csv(returns_path)

        return positions_pd, returns_pd

    def transform(self, input_data: Tuple[pd.DataFrame, pd.DataFrame]) -> Tuple[DataFrame, DataFrame]:
        """Cleanse Pandas DataFrames and convert to Snowpark DataFrames."""
        positions_pd, returns_pd = input_data

        # 1. Transform Positions Pandas DF
        self.logger.info("Transforming Positions Pandas DataFrame...")
        pos_df = positions_pd.copy()
        pos_df.columns = [c.upper() for c in pos_df.columns]
        pos_df["AS_OF_DATE"] = pd.to_datetime(pos_df["AS_OF_DATE"]).dt.strftime("%Y-%m-%d")
        pos_df["SOURCE_FILE"] = "positions.csv"

        # 2. Transform Factor Returns Pandas DF
        self.logger.info("Transforming Factor Returns Pandas DataFrame...")
        ret_df = returns_pd.copy()
        ret_df.columns = [c.upper() for c in ret_df.columns]
        ret_df["SCENARIO_DATE"] = pd.to_datetime(ret_df["SCENARIO_DATE"]).dt.strftime("%Y-%m-%d")
        ret_df["SOURCE_FILE"] = "factor_returns.csv"

        # 3. Convert Pandas DataFrames to Snowpark DataFrames
        self.logger.info("Converting Pandas DataFrames to Snowpark DataFrames...")
        snow_positions = (
            self.session.create_dataframe(pos_df)
            .with_column("AS_OF_DATE", to_date(col("AS_OF_DATE")))
            .with_column("INGESTED_AT", current_timestamp())
        )

        snow_returns = (
            self.session.create_dataframe(ret_df)
            .with_column("SCENARIO_DATE", to_date(col("SCENARIO_DATE")))
            .with_column("INGESTED_AT", current_timestamp())
        )

        return snow_positions, snow_returns

    def load(self, transformed_data: Tuple[DataFrame, DataFrame]) -> None:
        """Persist Snowpark DataFrames to RAW.RAW_POSITIONS and RAW.RAW_FACTOR_RETURNS."""
        snow_positions, snow_returns = transformed_data

        self.logger.info("Persisting positions to 'RAW.RAW_POSITIONS'...")
        snow_positions.write.mode("overwrite").save_as_table("RAW.RAW_POSITIONS")

        self.logger.info("Persisting factor returns to 'RAW.RAW_FACTOR_RETURNS'...")
        snow_returns.write.mode("overwrite").save_as_table("RAW.RAW_FACTOR_RETURNS")

        self.logger.info("Ingestion Layer execution completed successfully.")
