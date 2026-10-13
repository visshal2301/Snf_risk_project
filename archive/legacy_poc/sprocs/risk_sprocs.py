from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from src.pipelines.market_risk_pipeline import MarketRiskAssessmentPipeline


def run_market_risk_sproc(session: Session, as_of_date: str) -> str:
    """Stored Procedure entrypoint for running Market Risk calculations inside Snowflake."""
    pipeline = MarketRiskAssessmentPipeline(session, {}, as_of_date=as_of_date)
    pipeline.run()
    return f"Market Risk evaluation for AS_OF_DATE='{as_of_date}' completed successfully."


def register_sprocs(session: Session) -> None:
    """Registers Snowpark Stored Procedures in Snowflake."""
    import src
    session.add_import(src.__path__[0], import_path="src")
    session.sproc.register(
        func=run_market_risk_sproc,
        return_type=StringType(),
        input_types=[StringType()],
        name="SP_RUN_MARKET_RISK_EVALUATION",
        is_permanent=True,
        stage_location="@SYSTEM_SPROC_STAGE",
        replace=True,
        packages=["snowflake-snowpark-python", "pandas"]
    )

