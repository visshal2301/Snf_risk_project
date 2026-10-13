from unittest.mock import MagicMock
from src.pipelines.ingestion_pipeline import MarketRiskIngestionPipeline


def test_market_risk_ingestion_pipeline_instantiation():
    """Verify MarketRiskIngestionPipeline initializes correctly with config and session."""
    mock_session = MagicMock()
    config = {
        "pipeline": {
            "environment": "dev",
            "log_level": "DEBUG",
        }
    }

    pipeline = MarketRiskIngestionPipeline(
        session=mock_session,
        config=config,
    )

    assert pipeline.session == mock_session
    assert pipeline.config == config
