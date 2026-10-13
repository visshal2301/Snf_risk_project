from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import col, when, lit, current_timestamp
from src.pipelines.base_pipeline import BasePipeline


class RiskAssessmentPipeline(BasePipeline):
    """Production Risk Score ETL pipeline evaluating transaction & account anomalies."""

    def extract(self) -> DataFrame:
        """Fetch raw transaction records from Snowflake table."""
        self.logger.info("Extracting transactions data...")
        return self.session.table("RAW_TRANSACTIONS")

    def transform(self, input_df: DataFrame) -> DataFrame:
        """Calculates risk categories and flags high-risk transactions."""
        self.logger.info("Applying risk calculation transformation logic...")
        threshold = self.config.get("pipeline", {}).get("high_risk_threshold", 0.85)

        transformed_df = input_df.with_column(
            "RISK_CATEGORY",
            when(col("RISK_SCORE") >= threshold, lit("HIGH"))
            .when(col("RISK_SCORE") >= 0.50, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        ).with_column(
            "IS_ALERT_REQUIRED",
            when(col("RISK_SCORE") >= threshold, lit(True)).otherwise(lit(False))
        ).with_column(
            "PROCESSED_AT",
            current_timestamp()
        )

        return transformed_df

    def load(self, transformed_df: DataFrame) -> None:
        """Write processed risk scores to TARGET_RISK_SCORES table."""
        self.logger.info("Loading transformed risk assessment results into TARGET_RISK_SCORES table...")
        transformed_df.write.mode("append").save_as_table("TARGET_RISK_SCORES")
        self.logger.info("Data write complete.")
