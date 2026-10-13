"""
Unit Tests for Streamlit Dashboard Components
=============================================
Tests contagion simulation math, cascade round logic, and style generation.
"""

import pytest
import pandas as pd
from abor_risk.streamlit.contagion_engine import simulate_network_cascade, CAPITAL_BUFFER_RATIOS
from abor_risk.streamlit.styles import render_metric_card


def test_capital_buffer_ratios():
    assert 'AAA' in CAPITAL_BUFFER_RATIOS
    assert CAPITAL_BUFFER_RATIOS['AAA'] > CAPITAL_BUFFER_RATIOS['BBB']
    assert CAPITAL_BUFFER_RATIOS['BBB'] > CAPITAL_BUFFER_RATIOS['CCC']


def test_contagion_simulation_success():
    sample_data = pd.DataFrame([
        {
            'COUNTERPARTY_ID': 'CPTY_001',
            'COUNTERPARTY_NAME': 'Apex Prime Brokerage',
            'CREDIT_RATING': 'A',
            'NET_EXPOSURE_USD': 150_000_000.0,
            'SECTOR': 'Banking'
        },
        {
            'COUNTERPARTY_ID': 'CPTY_002',
            'COUNTERPARTY_NAME': 'Beacon Capital Markets',
            'CREDIT_RATING': 'BBB',
            'NET_EXPOSURE_USD': 120_000_000.0,
            'SECTOR': 'Non-Bank Financial'
        },
        {
            'COUNTERPARTY_ID': 'CPTY_003',
            'COUNTERPARTY_NAME': 'Crestline Sovereign Fund',
            'CREDIT_RATING': 'AAA',
            'NET_EXPOSURE_USD': 80_000_000.0,
            'SECTOR': 'Sovereign / Agency'
        }
    ])

    res = simulate_network_cascade(
        df_cpty=sample_data,
        initial_default_id='CPTY_001',
        lgd_pct=60.0,
        fire_sale_discount_pct=20.0
    )

    assert res['status'] == 'success'
    assert res['total_cptys'] == 3
    assert res['total_defaulted'] >= 1
    assert res['direct_firm_loss_usd'] == 150_000_000.0 * 0.60
    assert res['rounds_executed'] >= 1
    assert 'detailed_df' in res
    assert 'IS_DEFAULTED' in res['detailed_df'].columns


def test_contagion_simulation_empty():
    res = simulate_network_cascade(
        df_cpty=pd.DataFrame(),
        initial_default_id='CPTY_001'
    )
    assert res['status'] == 'error'


def test_render_metric_card():
    card_html = render_metric_card("Total AUM", "$1,000,000.00", subtitle="Book: $950,000", delta="+5.2%", delta_type="pos")
    assert "Total AUM" in card_html
    assert "$1,000,000.00" in card_html
    assert "metric-delta-pos" in card_html
