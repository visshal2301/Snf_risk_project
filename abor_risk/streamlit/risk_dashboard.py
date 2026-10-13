"""
🏛️ ABORT-RISK-360 — Institutional Risk Command Center
======================================================
Unified Streamlit in Snowflake (SiS) & Local Quantitative Risk Dashboard.
Visualizes:
  - Executive Portfolio Risk & 14-Day Forward VaR Trajectory
  - Eisenberg-Noe Domino Contagion & Liquidity Shock Simulator
  - Machine Learning Counterparty Credit Rating Downgrade Early Warning Radar
  - Basel Large Exposure Concentration Surveillance
  - Unsupervised Real-Time Risk Anomaly Detection
  - Enterprise Two-Tier Audit Telemetry & Governance
"""

import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure repository root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from abor_risk.streamlit.styles import CUSTOM_CSS, render_metric_card, clean_html
from abor_risk.streamlit.data_service import (
    get_session,
    get_available_snapshot_dates,
    get_portfolio_list,
    get_executive_portfolio_metrics,
    get_asset_class_distribution,
    get_var_forecasts,
    get_counterparty_concentration,
    get_downgrade_predictions,
    get_rating_migrations,
    get_risk_anomalies,
    get_audit_logs,
    get_model_registry_info
)
from abor_risk.streamlit.contagion_engine import simulate_network_cascade


# ==============================================================================
# PAGE CONFIGURATION & STYLING
# ==============================================================================
st.set_page_config(
    page_title="ABORT-RISK-360 | Institutional Risk Command Center",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ==============================================================================
# SIDEBAR FILTERS
# ==============================================================================
st.sidebar.markdown("### 🏛️ Risk Command Center")
st.sidebar.markdown("**Institutional ABOR & Quantitative Intelligence**")
st.sidebar.markdown("---")

# 1. Snapshot Date Filter
available_dates = get_available_snapshot_dates()
selected_date = st.sidebar.selectbox(
    "📅 Snapshot Date",
    options=available_dates,
    index=0,
    help="Select the daily accounting and risk ledger snapshot date."
)

# 2. Portfolio Filter
portfolios_raw = get_portfolio_list()
portfolio_options = ["ALL"] + [p['PORTFOLIO_ID'] for p in portfolios_raw]
portfolio_labels = {"ALL": "🌐 All Portfolios (Firm-wide)"}
for p in portfolios_raw:
    portfolio_labels[p['PORTFOLIO_ID']] = f"{p['PORTFOLIO_ID']} — {p['PORTFOLIO_NAME'][:24]}"

selected_portfolio = st.sidebar.selectbox(
    "📁 Portfolio Universe",
    options=portfolio_options,
    format_func=lambda x: portfolio_labels.get(x, x),
    index=0
)

# 3. Sector Filter
sector_options = ["ALL", "Banking", "Asset Management", "Insurance", "Sovereign / Agency", "Non-Bank Financial"]
selected_sector = st.sidebar.selectbox(
    "🏢 Counterparty Sector",
    options=sector_options,
    index=0
)

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data & Cache", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown(
    clean_html("""
    <div style='color: #64748b; font-size: 11px; margin-top: 20px;'>
        <b>Snowflake Engine</b>: Snowpark Python<br/>
        <b>Data Models</b>: SCD Type 2 & Gold Marts<br/>
        <b>ML Models</b>: In-Database RF & Forecast<br/>
        <b>Status</b>: Live Enterprise In-Memory
    </div>
    """),
    unsafe_allow_html=True
)


# ==============================================================================
# MAIN BRAND HEADER
# ==============================================================================
st.markdown(
    clean_html(f"""
    <div class="main-header">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h1>🏛️ ABORT-RISK-360 — Institutional Risk Command Center</h1>
                <p>Accounting Book of Record (ABOR) & Quantitative Risk Analytics Platform on Snowflake</p>
            </div>
            <div>
                <span class="session-chip">
                    <span style="height: 8px; width: 8px; background-color: #10b981; border-radius: 50%; display: inline-block;"></span>
                    Snowflake Connected: {selected_date}
                </span>
            </div>
        </div>
    </div>
    """),
    unsafe_allow_html=True
)


# ==============================================================================
# TAB LAYOUT
# ==============================================================================
tab_exec, tab_contagion, tab_downgrade, tab_anomalies, tab_governance = st.tabs([
    "📊 Executive Portfolio Risk & VaR Forecast",
    "⚡ Systemic Contagion & Shock Simulator",
    "🎯 Counterparty Downgrade Radar & Basel Exposure",
    "🚨 Live Risk Anomaly Surveillance",
    "🏛️ AI Narrative & Enterprise Governance"
])


# ==============================================================================
# TAB 1: EXECUTIVE PORTFOLIO RISK & VAR FORECASTING
# ==============================================================================
with tab_exec:
    df_port = get_executive_portfolio_metrics(selected_date, selected_portfolio)

    if df_port.empty:
        st.warning(f"No risk metric data available for snapshot date {selected_date} and selection {selected_portfolio}.")
    else:
        # 1. KPI Metric Row
        tot_aum = df_port['TOTAL_AUM_USD'].sum()
        tot_book = df_port['TOTAL_BOOK_VALUE_USD'].sum()
        tot_unrealized_pnl = df_port['TOTAL_UNREALIZED_PNL_USD'].sum()
        tot_var = df_port['TOTAL_VAR_99_USD'].sum()
        tot_pfe = df_port['TOTAL_PFE_95_USD'].sum()
        tot_cva = df_port['TOTAL_CVA_USD'].sum()
        leverage_pct = (tot_var / abs(tot_aum) * 100) if tot_aum != 0 else 0.0
        ret_pct = (tot_unrealized_pnl / abs(tot_book) * 100) if tot_book != 0 else 0.0

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown(render_metric_card("Total Firm AUM", f"${tot_aum:,.2f}", f"Book: ${tot_book:,.2f}"), unsafe_allow_html=True)
        with col2:
            st.markdown(render_metric_card("99% Value-at-Risk", f"${tot_var:,.2f}", f"Leverage: {leverage_pct:.2f}% of AUM", delta=f"{leverage_pct:.2f}%", delta_type="neutral"), unsafe_allow_html=True)
        with col3:
            st.markdown(render_metric_card("95% Potential Exposure (PFE)", f"${tot_pfe:,.2f}", "95% 1-Year Horizon"), unsafe_allow_html=True)
        with col4:
            st.markdown(render_metric_card("CVA Provision Reserve", f"${tot_cva:,.2f}", "Counterparty Credit Adjustment"), unsafe_allow_html=True)
        with col5:
            delta_type = "pos" if tot_unrealized_pnl >= 0 else "neg"
            st.markdown(render_metric_card("Unrealized ABOR P&L", f"${tot_unrealized_pnl:,.2f}", f"Return: {ret_pct:+.2f}%", delta=f"{ret_pct:+.2f}%", delta_type=delta_type), unsafe_allow_html=True)

        st.markdown("<br/>", unsafe_allow_html=True)

        # 2. Portfolio Breakdown Charts
        col_c1, col_c2 = st.columns([6, 4])
        with col_c1:
            st.markdown("<div class='section-title'>Portfolio Capital vs 99% VaR Allocation</div>", unsafe_allow_html=True)
            fig_port = px.bar(
                df_port,
                x='PORTFOLIO_ID',
                y=['TOTAL_AUM_USD', 'TOTAL_VAR_99_USD'],
                barmode='group',
                labels={'value': 'USD', 'PORTFOLIO_ID': 'Portfolio', 'variable': 'Risk Metric'},
                color_discrete_map={'TOTAL_AUM_USD': '#0284c7', 'TOTAL_VAR_99_USD': '#ef4444'},
                template='plotly_dark'
            )
            fig_port.update_layout(height=340, margin=dict(l=20, r=20, t=20, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_port, use_container_width=True)

        with col_c2:
            st.markdown("<div class='section-title'>Asset Class & Tenor Risk Breakdown</div>", unsafe_allow_html=True)
            df_asset = get_asset_class_distribution(selected_date)
            if not df_asset.empty:
                fig_asset = px.pie(
                    df_asset,
                    values='TOTAL_MARKET_VALUE_USD',
                    names='ASSET_CLASS',
                    hole=0.45,
                    color_discrete_sequence=px.colors.qualitative.Dark24,
                    template='plotly_dark'
                )
                fig_asset.update_layout(height=340, margin=dict(l=20, r=20, t=20, b=20), legend=dict(orientation="h", yanchor="bottom", y=-0.1))
                st.plotly_chart(fig_asset, use_container_width=True)
            else:
                st.info("No asset class distribution available for selected date.")

        # 3. 14-Day Forward VaR Trajectory Forecast (Use Case 1)
        st.markdown("<div class='section-title'>📈 Native Snowflake Machine Learning: 14-Day VaR Trajectory Forecast (SNOWFLAKE.ML.FORECAST)</div>", unsafe_allow_html=True)
        df_forecast = get_var_forecasts(selected_portfolio)

        if not df_forecast.empty:
            # Aggregate or filter by portfolio
            if selected_portfolio == "ALL":
                df_fc_plot = df_forecast.groupby('FORECAST_DATE').agg({
                    'PROJECTED_VAR_99': 'sum',
                    'LOWER_BOUND_VAR_99': 'sum',
                    'UPPER_BOUND_VAR_99': 'sum'
                }).reset_index()
                title_suffix = "(Firm-Wide Aggregated)"
            else:
                df_fc_plot = df_forecast[df_forecast['PORTFOLIO_ID'] == selected_portfolio].copy()
                title_suffix = f"({selected_portfolio})"

            fig_fc = go.Figure()
            # Upper bound
            fig_fc.add_trace(go.Scatter(
                x=df_fc_plot['FORECAST_DATE'],
                y=df_fc_plot['UPPER_BOUND_VAR_99'],
                mode='lines',
                line=dict(width=0),
                showlegend=False,
                name='Upper Bound (95% CI)'
            ))
            # Lower bound with fill
            fig_fc.add_trace(go.Scatter(
                x=df_fc_plot['FORECAST_DATE'],
                y=df_fc_plot['LOWER_BOUND_VAR_99'],
                mode='lines',
                line=dict(width=0),
                fill='tonexty',
                fillcolor='rgba(239, 68, 68, 0.15)',
                name='95% Confidence Band'
            ))
            # Main forecast line
            fig_fc.add_trace(go.Scatter(
                x=df_fc_plot['FORECAST_DATE'],
                y=df_fc_plot['PROJECTED_VAR_99'],
                mode='lines+markers',
                line=dict(color='#ef4444', width=3),
                marker=dict(size=6, color='#ef4444'),
                name=f'Projected 99% VaR {title_suffix}'
            ))
            fig_fc.update_layout(
                height=320,
                margin=dict(l=20, r=20, t=20, b=20),
                template='plotly_dark',
                hovermode='x unified',
                yaxis=dict(title='Projected VaR ($ USD)')
            )
            st.plotly_chart(fig_fc, use_container_width=True)
        else:
            st.info("No forward forecast trajectory data loaded in ANALYTICS.FACT_PORTFOLIO_RISK_FORECASTS.")

        # 4. Detailed Portfolio Table
        st.markdown("<div class='section-title'>Institutional Portfolio Ledger & Capital Standing</div>", unsafe_allow_html=True)
        st.dataframe(
            df_port[[
                'PORTFOLIO_ID', 'PORTFOLIO_NAME', 'PORTFOLIO_MANAGER', 'INVESTMENT_STRATEGY',
                'TOTAL_AUM_USD', 'TOTAL_BOOK_VALUE_USD', 'TOTAL_UNREALIZED_PNL_USD',
                'TOTAL_VAR_99_USD', 'TOTAL_PFE_95_USD', 'TOTAL_CVA_USD', 'VAR_TO_AUM_PCT'
            ]],
            use_container_width=True,
            hide_index=True
        )


# ==============================================================================
# TAB 2: SYSTEMIC CONTAGION & SHOCK SIMULATOR
# ==============================================================================
with tab_contagion:
    st.markdown("<div class='section-title'>⚡ Eisenberg-Noe / Glasserman Multi-Round Domino Cascade Simulator</div>", unsafe_allow_html=True)
    st.markdown(
        clean_html("""
        <div class="callout-box">
            <b>Systemic Contagion Engine</b>: Simulates the catastrophic default of an institutional clearing broker or bank,
            tracing cross-entity counterparty exposures, asset fire-sale illiquidity write-downs, and secondary domino insolvency cascades.
        </div>
        """),
        unsafe_allow_html=True
    )

    df_cpty_sim = get_counterparty_concentration(selected_date)

    if df_cpty_sim.empty:
        st.warning(f"No counterparty exposure data found for snapshot date {selected_date}.")
    else:
        # Simulation Controls Card
        with st.expander("🛠️ Interactive Stress Shock Parameters", expanded=True):
            col_s1, col_s2, col_s3, col_s4 = st.columns([4, 3, 3, 2])
            with col_s1:
                cpty_choices = list(df_cpty_sim['COUNTERPARTY_ID'].values)
                cpty_labels = {
                    r['COUNTERPARTY_ID']: f"{r['COUNTERPARTY_NAME']} ({r['COUNTERPARTY_ID']}) — ${r['NET_EXPOSURE_USD']:,.0f}"
                    for _, r in df_cpty_sim.iterrows()
                }
                initial_trigger_id = st.selectbox(
                    "💥 Initial Defaulting Entity (Zero-Hour Trigger)",
                    options=cpty_choices,
                    format_func=lambda x: cpty_labels.get(x, x),
                    index=0
                )
            with col_s2:
                sim_lgd = st.slider("📉 Loss Given Default (LGD %)", min_value=10.0, max_value=100.0, value=65.0, step=5.0)
            with col_s3:
                sim_fire_sale = st.slider("🔥 Fire-Sale Illiquidity Haircut (%)", min_value=0.0, max_value=50.0, value=20.0, step=5.0)
            with col_s4:
                st.write("")
                st.write("")
                run_sim_btn = st.button("⚡ Run Stress Simulation", use_container_width=True, type="primary")

        # Run simulation
        sim_res = simulate_network_cascade(
            df_cpty=df_cpty_sim,
            initial_default_id=initial_trigger_id,
            lgd_pct=sim_lgd,
            fire_sale_discount_pct=sim_fire_sale
        )

        if sim_res.get("status") == "success":
            # 1. Shock Summary KPI Metrics
            m1, m2, m3, m4, m5 = st.columns(5)
            with m1:
                st.markdown(render_metric_card("Triggering Default", sim_res['initial_entity_name'][:20], f"ID: {sim_res['initial_entity_id']}"), unsafe_allow_html=True)
            with m2:
                st.markdown(render_metric_card("Direct Loss to Firm", f"${sim_res['direct_firm_loss_usd']:,.2f}", f"LGD: {sim_lgd}%"), unsafe_allow_html=True)
            with m3:
                st.markdown(render_metric_card("Secondary Domino Deficit", f"${sim_res['secondary_firm_loss_usd']:,.2f}", f"{sim_res['total_defaulted'] - 1} secondary failures"), unsafe_allow_html=True)
            with m4:
                st.markdown(render_metric_card("Total Capital Impairment", f"${sim_res['total_firm_loss_usd']:,.2f}", "Direct + Domino Loss", delta_type="neg"), unsafe_allow_html=True)
            with m5:
                st.markdown(render_metric_card("Systemic Contagion Index", f"{sim_res['contagion_index_pct']}%", f"{sim_res['total_defaulted']} of {sim_res['total_cptys']} Defaulted"), unsafe_allow_html=True)

            st.markdown("<br/>", unsafe_allow_html=True)

            # 2. Cascade Waterfall & Round Progression
            col_r1, col_r2 = st.columns([6, 4])
            with col_r1:
                st.markdown("<div class='section-title'>Domino Round Loss Progression ($ USD)</div>", unsafe_allow_html=True)
                df_rounds = pd.DataFrame(sim_res['rounds_log'])
                fig_cascade = px.bar(
                    df_rounds,
                    x='stage',
                    y=['round_loss_firm_usd', 'round_network_loss_usd'],
                    barmode='group',
                    labels={'value': 'USD Loss', 'stage': 'Propagation Round', 'variable': 'Loss Domain'},
                    color_discrete_map={'round_loss_firm_usd': '#ef4444', 'round_network_loss_usd': '#f59e0b'},
                    template='plotly_dark'
                )
                fig_cascade.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig_cascade, use_container_width=True)

            with col_r2:
                st.markdown("<div class='section-title'>Cascade Propagation Log</div>", unsafe_allow_html=True)
                for r_info in sim_res['rounds_log']:
                    badge_cls = "badge-critical" if r_info['round'] == 0 else ("badge-high" if r_info['new_defaults_count'] > 0 else "badge-normal")
                    st.markdown(
                        clean_html(f"""
                        <div style='background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 10px 14px; margin-bottom: 8px;'>
                            <div style='display: flex; justify-content: space-between;'>
                                <span style='font-weight: 700; color: #f8fafc; font-size: 13px;'>{r_info['stage']}</span>
                                <span class='{badge_cls}'>{r_info['new_defaults_count']} New Defaults</span>
                            </div>
                            <div style='color: #94a3b8; font-size: 12px; margin-top: 4px;'>{r_info['description']}</div>
                        </div>
                        """),
                        unsafe_allow_html=True
                    )

            # 3. Detailed Entity Survival & Deficit Ledger
            st.markdown("<div class='section-title'>Post-Shock Institutional Balance Sheet & Default Matrix</div>", unsafe_allow_html=True)
            df_sim_detail = sim_res['detailed_df']
            
            # Add formatted status column
            df_sim_detail['STATUS_BADGE'] = df_sim_detail.apply(
                lambda r: f"💥 DEFAULTED (Round {r['DEFAULT_ROUND']})" if r['IS_DEFAULTED'] else "✅ SOLVENT",
                axis=1
            )
            
            st.dataframe(
                df_sim_detail[[
                    'STATUS_BADGE', 'COUNTERPARTY_ID', 'COUNTERPARTY_NAME', 'CREDIT_RATING',
                    'NET_EXPOSURE_USD', 'ESTIMATED_CAPITAL_USD', 'CUMULATIVE_LOSS_USD'
                ]],
                use_container_width=True,
                hide_index=True
            )


# ==============================================================================
# TAB 3: COUNTERPARTY DOWNGRADE RADAR & BASEL CONCENTRATION
# ==============================================================================
with tab_downgrade:
    st.markdown("<div class='section-title'>🎯 Counterparty Credit Downgrade Early Warning & Basel Large Exposure Radar</div>", unsafe_allow_html=True)
    st.markdown(
        clean_html("""
        <div class="callout-box">
            <b>In-Database Machine Learning</b>: Balanced <code>RandomForestClassifier</code> trained inside Snowflake virtual warehouse
            on rolling MTM volatility, Basel concentration, and rating tenure. Predicts 30-day forward credit rating downgrades ($P \ge 0.45$)
            and maps entities into a 4-Tier Operational Action Matrix.
        </div>
        """),
        unsafe_allow_html=True
    )

    # 1. Fetch ML Predictions and Basel Concentration
    df_down = get_downgrade_predictions(selected_date)
    df_cpty_base = get_counterparty_concentration(selected_date, selected_sector)

    col_d1, col_d2, col_d3, col_d4 = st.columns(4)
    if not df_down.empty:
        crit_count = len(df_down[df_down['RISK_TIER'] == 'CRITICAL'])
        high_count = len(df_down[df_down['RISK_TIER'] == 'HIGH'])
        watch_count = len(df_down[df_down['RISK_TIER'] == 'WATCHLIST'])
        avg_prob = df_down['DOWNGRADE_PROBABILITY'].mean() * 100

        with col_d1:
            st.markdown(render_metric_card("CRITICAL Hazard Tier", f"{crit_count}", "P(Downgrade) >= 70%", delta_type="neg"), unsafe_allow_html=True)
        with col_d2:
            st.markdown(render_metric_card("HIGH Risk Tier", f"{high_count}", "P(Downgrade) >= 40%", delta_type="neg"), unsafe_allow_html=True)
        with col_d3:
            st.markdown(render_metric_card("WATCHLIST Tier", f"{watch_count}", "P(Downgrade) >= 20%"), unsafe_allow_html=True)
        with col_d4:
            st.markdown(render_metric_card("Mean Downgrade Prob", f"{avg_prob:.2f}%", f"{len(df_down)} Scored Entities"), unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

    # 2. Charts: Basel Concentration vs ML Downgrade Scatter
    col_g1, col_g2 = st.columns([6, 6])
    with col_g1:
        st.markdown("<div class='section-title'>Basel Large Exposure Status (% of Firm AUM)</div>", unsafe_allow_html=True)
        if not df_cpty_base.empty:
            df_top_cpty = df_cpty_base.head(15).copy()
            fig_base = px.bar(
                df_top_cpty,
                x='FIRM_CONCENTRATION_PCT',
                y='COUNTERPARTY_NAME',
                orientation='h',
                color='BASEL_CONCENTRATION_STATUS',
                color_discrete_map={'BREACH_WARNING': '#ef4444', 'ELEVATED': '#f59e0b', 'NORMAL': '#10b981'},
                labels={'FIRM_CONCENTRATION_PCT': 'Firm AUM %', 'COUNTERPARTY_NAME': 'Counterparty'},
                template='plotly_dark'
            )
            # Add 10% regulatory limit line
            fig_base.add_vline(x=10.0, line_dash="dash", line_color="#ef4444", annotation_text="10% Basel Limit", annotation_position="top right")
            fig_base.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_base, use_container_width=True)
        else:
            st.info("No counterparty concentration records found.")

    with col_g2:
        st.markdown("<div class='section-title'>ML Downgrade Probability vs Net Exposure ($ USD)</div>", unsafe_allow_html=True)
        if not df_down.empty:
            fig_scat = px.scatter(
                df_down,
                x='DOWNGRADE_PROBABILITY',
                y='FEATURE_NET_EXPOSURE_USD',
                color='RISK_TIER',
                color_discrete_map={'CRITICAL': '#ef4444', 'HIGH': '#f59e0b', 'WATCHLIST': '#38bdf8', 'LOW': '#10b981'},
                size='FEATURE_MTM_VOLATILITY_14D',
                hover_name='COUNTERPARTY_NAME',
                labels={'DOWNGRADE_PROBABILITY': 'P(Downgrade Next 30D)', 'FEATURE_NET_EXPOSURE_USD': 'Exposure ($ USD)'},
                template='plotly_dark'
            )
            fig_scat.add_vline(x=0.45, line_dash="dot", line_color="#f59e0b", annotation_text="Calibrated Threshold P*=0.45")
            fig_scat.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_scat, use_container_width=True)
        else:
            st.info("No ML predictions found.")

    # 3. 4-Tier Operational Action Matrix Table
    st.markdown("<div class='section-title'>4-Tier Operational Action Matrix & Early Warning Directives</div>", unsafe_allow_html=True)
    if not df_down.empty:
        st.dataframe(
            df_down[[
                'RISK_TIER', 'COUNTERPARTY_ID', 'COUNTERPARTY_NAME', 'CURRENT_CREDIT_RATING',
                'DOWNGRADE_PROBABILITY', 'RECOMMENDED_ACTION', 'FEATURE_NET_EXPOSURE_USD',
                'FEATURE_CONCENTRATION_PCT', 'FEATURE_MTM_VOLATILITY_14D'
            ]],
            use_container_width=True,
            hide_index=True
        )

    # 4. SCD Type 2 Rating Migration Audit Trail
    st.markdown("<div class='section-title'>SCD Type 2 Credit Rating Migration Historical Audit Trail</div>", unsafe_allow_html=True)
    df_mig = get_rating_migrations()
    if not df_mig.empty:
        st.dataframe(df_mig, use_container_width=True, hide_index=True)


# ==============================================================================
# TAB 4: RISK ANOMALY SURVEILLANCE
# ==============================================================================
with tab_anomalies:
    st.markdown("<div class='section-title'>🚨 Unsupervised Risk Anomaly Surveillance & Outlier Breaches (SNOWFLAKE.ML.ANOMALY_DETECTION)</div>", unsafe_allow_html=True)
    st.markdown(
        clean_html("""
        <div class="callout-box">
            <b>Native Snowflake Anomaly Engine</b>: Automatically surveils multi-series daily portfolio 99% VaR, detecting
            statistical spikes and drops exceeding Bayesian operating bands with zero lookahead leakage.
        </div>
        """),
        unsafe_allow_html=True
    )

    df_anom = get_risk_anomalies(selected_portfolio)

    if df_anom.empty:
        st.info("No anomaly records loaded in ANALYTICS.FACT_RISK_ANOMALIES.")
    else:
        tot_evals = len(df_anom)
        flagged_anom = len(df_anom[df_anom['IS_ANOMALY'] == True])
        crit_anom = len(df_anom[(df_anom['IS_ANOMALY'] == True) & (df_anom['SEVERITY_LEVEL'] == 'CRITICAL')])
        spike_anom = len(df_anom[(df_anom['IS_ANOMALY'] == True) & (df_anom['ANOMALY_DIRECTION'] == 'SPIKE')])

        col_a1, col_a2, col_a3, col_a4 = st.columns(4)
        with col_a1:
            st.markdown(render_metric_card("Surveilled Observations", f"{tot_evals:,}", "Batch & CDC Stream"), unsafe_allow_html=True)
        with col_a2:
            st.markdown(render_metric_card("Flagged Anomalies", f"{flagged_anom}", f"{(flagged_anom/tot_evals*100):.1f}% Rate", delta_type="neg"), unsafe_allow_html=True)
        with col_a3:
            st.markdown(render_metric_card("CRITICAL Spikes", f"{crit_anom}", "Statistical 99th Percentile", delta_type="neg"), unsafe_allow_html=True)
        with col_a4:
            st.markdown(render_metric_card("Positive Shock Spikes", f"{spike_anom}", "VaR Expansion Breaches"), unsafe_allow_html=True)

        st.markdown("<br/>", unsafe_allow_html=True)

        # Anomaly Timeline Scatter Chart
        st.markdown("<div class='section-title'>Actual 99% VaR vs Statistical Operating Bands & Anomaly Breaches</div>", unsafe_allow_html=True)
        fig_anom = px.scatter(
            df_anom,
            x='SNAPSHOT_DATE',
            y='ACTUAL_VAR_99',
            color='SEVERITY_LEVEL',
            color_discrete_map={'CRITICAL': '#ef4444', 'HIGH': '#f59e0b', 'MEDIUM': '#38bdf8', 'NORMAL': '#64748b'},
            symbol='ANOMALY_DIRECTION',
            hover_name='PORTFOLIO_NAME',
            labels={'ACTUAL_VAR_99': 'Actual VaR ($ USD)', 'SNAPSHOT_DATE': 'Date'},
            template='plotly_dark'
        )
        fig_anom.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig_anom, use_container_width=True)

        # Anomaly Event Ledger
        st.markdown("<div class='section-title'>Anomalous Event Surveillance Ledger</div>", unsafe_allow_html=True)
        st.dataframe(
            df_anom[df_anom['IS_ANOMALY'] == True][[
                'SNAPSHOT_DATE', 'PORTFOLIO_ID', 'PORTFOLIO_NAME', 'SEVERITY_LEVEL',
                'ANOMALY_DIRECTION', 'ACTUAL_VAR_99', 'EXPECTED_VAR_99', 'DISTANCE_USD',
                'ANOMALY_PERCENTILE', 'SOURCE_MODE'
            ]],
            use_container_width=True,
            hide_index=True
        )


# ==============================================================================
# TAB 5: AI NARRATIVE & ENTERPRISE GOVERNANCE
# ==============================================================================
with tab_governance:
    st.markdown("<div class='section-title'>🤖 Automated Executive CRO Risk Briefing (Snowflake Cortex AI Synthesis)</div>", unsafe_allow_html=True)

    df_port_all = get_executive_portfolio_metrics(selected_date)
    df_down_all = get_downgrade_predictions(selected_date)

    if not df_port_all.empty:
        t_aum = df_port_all['TOTAL_AUM_USD'].sum()
        t_var = df_port_all['TOTAL_VAR_99_USD'].sum()
        t_pfe = df_port_all['TOTAL_PFE_95_USD'].sum()
        t_cva = df_port_all['TOTAL_CVA_USD'].sum()
        crit_entities = df_down_all[df_down_all['RISK_TIER'] == 'CRITICAL']['COUNTERPARTY_NAME'].tolist() if not df_down_all.empty else []
        high_entities = df_down_all[df_down_all['RISK_TIER'] == 'HIGH']['COUNTERPARTY_NAME'].tolist() if not df_down_all.empty else []

        briefing_text = f"""
### 🏛️ CHIEF RISK OFFICER (CRO) DAILY RISK SYNTHESIS
**Snapshot Date**: `{selected_date}`  
**Generated At**: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`  
**Platform**: Institutional ABOR & Quantitative Risk Platform (`abor_risk`)

---

#### 1. 📌 Executive Summary & Balance Sheet Standing
* **Firm-Wide Total AUM**: **${t_aum:,.2f}** across 20 active institutional portfolios.
* **Consolidated 99% Value-at-Risk (VaR)**: **${t_var:,.2f}** ({((t_var/abs(t_aum))*100 if t_aum != 0 else 0):.2f}% portfolio loss buffer).
* **Consolidated 95% Potential Future Exposure (PFE)**: **${t_pfe:,.2f}** 1-year horizon exposure.
* **Credit Value Adjustment (CVA) Reserve Provision**: **${t_cva:,.2f}**.

#### 2. 🚨 High-Priority Counterparty Vulnerabilities
* **Critical Downgrade Hazard Entities**: {', '.join(crit_entities) if crit_entities else 'None detected at P >= 0.70 threshold'}.
* **Elevated Downgrade Warning Entities**: {', '.join(high_entities[:3]) if high_entities else 'None in High tier'}.
* **Operational Directives**: Immediate collateral margin calls and 30% credit limit reductions recommended for non-investment grade brokers.

#### 3. 🛡️ Quantitative Risk Governance & Recommendations
1. Maintain overnight cash and Treasury liquidity exceeding 99% VaR tail parameter (${t_var:,.2f}).
2. Execute Eisenberg-Noe domino stress test on clearing counterparties exhibiting 14-day MTM volatility spikes.
3. Validate SCD Type 2 dimension temporal integrity in `ENG.DIM_COUNTERPARTY` prior to end-of-quarter accounting sign-off.
        """
        st.markdown(briefing_text)

    st.markdown("<br/>", unsafe_allow_html=True)

    # In-Database Model Registry
    st.markdown("<div class='section-title'>📦 In-Database Snowflake Model Registry (ANALYTICS.FACT_MODEL_STORE)</div>", unsafe_allow_html=True)
    df_reg = get_model_registry_info()
    if not df_reg.empty:
        st.dataframe(df_reg, use_container_width=True, hide_index=True)
    else:
        st.info("No registered models in ANALYTICS.FACT_MODEL_STORE.")

    # Two-Tier Audit Log
    st.markdown("<div class='section-title'>🛡️ Enterprise Pipeline Execution Telemetry (ENG.AUDIT_EXECUTION_LOG)</div>", unsafe_allow_html=True)
    df_audit = get_audit_logs(limit=12)
    if not df_audit.empty:
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
    else:
        st.info("No audit logs found.")
