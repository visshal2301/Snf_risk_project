import uuid
from typing import Any, Dict, Optional, Tuple
from snowflake.snowpark import DataFrame, Session
from src.pipelines.base_pipeline import BasePipeline


class MarketRiskAssessmentPipeline(BasePipeline):
    """Transformation & Analytics Pipeline performing historical-simulation market-risk revaluation."""

    def __init__(
        self,
        session: Session,
        config: Dict[str, Any],
        as_of_date: str = "2026-09-25",
        run_id: Optional[str] = None,
    ):
        super().__init__(session, config)
        self.as_of_date = as_of_date
        self.run_id = run_id or f"RUN_{uuid.uuid4().hex[:8].upper()}"

    def extract(self) -> Tuple[DataFrame, DataFrame]:
        """Extract position exposures and factor returns from RAW schema."""
        self.logger.info("Extracting RAW.RAW_POSITIONS and RAW.RAW_FACTOR_RETURNS tables...")
        pos_df = self.session.table("RAW.RAW_POSITIONS")
        returns_df = self.session.table("RAW.RAW_FACTOR_RETURNS")
        return pos_df, returns_df

    def transform(self, input_data: Tuple[DataFrame, DataFrame]) -> Tuple[DataFrame, DataFrame]:
        """Compute 250-day historical scenario PnL, 99% VaR, and Expected Shortfall (ES)."""
        self.logger.info(
            f"Computing market risk transformation for AS_OF_DATE='{self.as_of_date}', RUN_ID='{self.run_id}'..."
        )

        # 1. Historical Scenario PnL Calculation Query
        query_scenarios = f"""
        WITH POS AS (
            SELECT PORTFOLIO, RISK_FACTOR, SIGNED_MARKET_VALUE
            FROM RAW.RAW_POSITIONS
            WHERE AS_OF_DATE = '{self.as_of_date}'
        ), DATES AS (
            SELECT SCENARIO_DATE,
                   ROW_NUMBER() OVER (ORDER BY SCENARIO_DATE DESC) AS RN
            FROM (
                SELECT DISTINCT SCENARIO_DATE
                FROM RAW.RAW_FACTOR_RETURNS
                WHERE SCENARIO_DATE <= '{self.as_of_date}'
            )
        )
        SELECT '{self.run_id}' AS RUN_ID,
               '{self.as_of_date}'::DATE AS AS_OF_DATE,
               P.PORTFOLIO,
               R.SCENARIO_DATE,
               SUM(P.SIGNED_MARKET_VALUE * R.FACTOR_RETURN)::NUMBER(38,2) AS PNL_USD,
               (-SUM(P.SIGNED_MARKET_VALUE * R.FACTOR_RETURN))::NUMBER(38,2) AS LOSS_USD,
               CURRENT_TIMESTAMP() AS CALCULATED_AT
        FROM POS P
        JOIN RAW.RAW_FACTOR_RETURNS R ON R.RISK_FACTOR = P.RISK_FACTOR
        JOIN DATES D ON D.SCENARIO_DATE = R.SCENARIO_DATE AND D.RN <= 250
        GROUP BY P.PORTFOLIO, R.SCENARIO_DATE
        """
        scenario_pnl_df = self.session.sql(query_scenarios)

        # 2. 99% VaR and Expected Shortfall Metrics Calculation Query
        query_metrics = f"""
        WITH RUN_SCENARIOS AS ({query_scenarios}),
        VARS AS (
            SELECT RUN_ID, AS_OF_DATE, PORTFOLIO, COUNT(*) AS SCENARIO_COUNT,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY LOSS_USD) AS VAR_USD,
                   MAX(CALCULATED_AT) AS CALCULATED_AT
            FROM RUN_SCENARIOS
            GROUP BY RUN_ID, AS_OF_DATE, PORTFOLIO
        ), TAIL AS (
            SELECT V.RUN_ID, V.PORTFOLIO, AVG(S.LOSS_USD) AS ES_USD
            FROM VARS V
            JOIN RUN_SCENARIOS S
                ON S.RUN_ID = V.RUN_ID
               AND S.PORTFOLIO = V.PORTFOLIO
               AND S.LOSS_USD >= V.VAR_USD
            GROUP BY V.RUN_ID, V.PORTFOLIO
        )
        SELECT V.RUN_ID, V.AS_OF_DATE, V.PORTFOLIO, 0.99 AS CONFIDENCE, 1 AS HORIZON_DAYS,
               V.SCENARIO_COUNT, V.VAR_USD, T.ES_USD, V.CALCULATED_AT
        FROM VARS V
        JOIN TAIL T ON T.RUN_ID = V.RUN_ID AND T.PORTFOLIO = V.PORTFOLIO
        """
        risk_metrics_df = self.session.sql(query_metrics)

        return scenario_pnl_df, risk_metrics_df

    def load(self, transformed_data: Tuple[DataFrame, DataFrame]) -> None:
        """Persist results to ANALYTICS.RISK_SCENARIO_PNL and ANALYTICS.RISK_METRICS."""
        scenario_pnl_df, risk_metrics_df = transformed_data

        self.logger.info("Persisting scenario PnL into 'ANALYTICS.RISK_SCENARIO_PNL'...")
        scenario_pnl_df.write.mode("append").save_as_table("ANALYTICS.RISK_SCENARIO_PNL")

        self.logger.info("Persisting metric summary into 'ANALYTICS.RISK_METRICS'...")
        risk_metrics_df.write.mode("append").save_as_table("ANALYTICS.RISK_METRICS")

        self.logger.info("Analytics Layer execution completed successfully.")
