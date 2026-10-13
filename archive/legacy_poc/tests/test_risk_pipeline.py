from src.udfs.risk_udfs import calculate_risk_score


def test_calculate_risk_score():
    """Verify calculate_risk_score logic."""
    assert calculate_risk_score(100.0, 2) == (100.0 * 0.0001) + (2 * 0.15)
    assert calculate_risk_score(0.0, 0) == 0.0
    assert calculate_risk_score(1000000.0, 100) == 1.0  # Should clamp to 1.0


def test_calculate_risk_score_none_handling():
    """Verify handling of None values."""
    assert calculate_risk_score(None, 2) == 0.0
    assert calculate_risk_score(100.0, None) == 0.0
