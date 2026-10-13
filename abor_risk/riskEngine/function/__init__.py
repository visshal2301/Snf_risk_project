"""
Risk Engine Function Subpackage Initializer
============================================
Contains Python modules that define and execute Snowflake UDF creation logic.
"""

from abor_risk.riskEngine.function.create_risk_weight_udf import create_fn_get_credit_risk_weight_udf

__all__ = ["create_fn_get_credit_risk_weight_udf"]
