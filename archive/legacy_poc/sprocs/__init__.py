"""Snowpark Python Stored Procedures module."""

from src.sprocs.risk_sprocs import run_market_risk_sproc, register_sprocs

__all__ = ["run_market_risk_sproc", "register_sprocs"]
