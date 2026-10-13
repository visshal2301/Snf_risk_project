from snowflake.snowpark import Session
from snowflake.snowpark.types import FloatType, IntegerType


def calculate_risk_score(amount: float, velocity_count: int) -> float:
    """Python function logic to calculate risk score given amount & velocity."""
    if amount is None or velocity_count is None:
        return 0.0
    
    score = (amount * 0.0001) + (velocity_count * 0.15)
    return min(float(score), 1.0)


def register_udfs(session: Session) -> None:
    """Registers Snowpark UDFs into Snowflake environment."""
    session.udf.register(
        func=calculate_risk_score,
        return_type=FloatType(),
        input_types=[FloatType(), IntegerType()],
        name="CALCULATE_RISK_SCORE",
        is_permanent=True,
        stage_location="@SYSTEM_UDF_STAGE",
        replace=True
    )
