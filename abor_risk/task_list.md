# 📋 Institutional ABOR Risk Platform — Master Task Checklist

**Project**: Institutional ABOR & Quantitative Risk Platform (`abor_risk`)  
**Status Date**: October 2026  

---

## 🟢 Phase 1: Environment & Virtualenv Setup
- [x] Configure Python 3.10/3.11 virtual environment (`.venv`)
- [x] Install dependencies (`snowflake-snowpark-python`, `scikit-learn`, `pytest`, `pyyaml`, `python-dotenv`)
- [x] Configure credentials in `.env` (`yjanmbj-em54153`, `padaialgo`, `ACCOUNTADMIN`)
- [x] Verify live Snowflake connection via Snowpark Session Manager

---

## 🟢 Phase 2: Core ABOR Medallion Layer (Silver & Gold)
- [x] **DDL Migration**: Deploy schemas (`ENG`, `ANALYTICS`) and SCD Type 2 tables via `sql/migrations/V2.0.0__abor_risk_schema.sql`
- [x] **Synthetic Parquet Generation**: Generate 9 months of daily business snapshots across 50 counterparties, 15 instruments, and 5 portfolios (`fileGeneration/`)
- [x] **JSON Schema Contracts**: Enforce strict pre-ingestion contract validation (`metadata/*_schema.json`)
- [x] **SCD Type 2 Dimension Ingestion**:
  - [x] `DIM_COUNTERPARTY` (Historical rating migrations with zero-leakage validity dates)
  - [x] `DIM_INSTRUMENT` (Asset class taxonomy & tenors)
  - [x] `DIM_PORTFOLIO` (Manager & strategy tracking)
- [x] **Fact Table Ingestion**:
  - [x] `FACT_DAILY_POSITIONS_ABOR` (Daily accounting ledger & MTM valuations)
  - [x] `FACT_SETTLED_TRANSACTIONS` (Settled cash flows & turnover volumes)
- [x] **In-Database Daily Risk Engine SPROC**:
  - [x] Deploy `SP_CALCULATE_DAILY_RISK_METRICS` calculating 99% VaR, 95% PFE, and CVA in Snowflake warehouse compute
- [x] **Analytics Consumption Layer (Gold Views)**:
  - [x] `ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK`
  - [x] `ANALYTICS.VW_COUNTERPARTY_CONCENTRATION`
  - [x] `ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION`
  - [x] `ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS`
  - [x] `ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION`

---

## 🟢 Phase 3: Cortex Machine Learning & Risk Intelligence Suite
- [x] **Use Case 1: VaR Anomaly & Backtesting Breach Detector**
  - [x] Documented in `cortex/1.var_aum_trajectory/USE_CASE_EXPLANATION.md` & `IMPLEMENTATION_STEPS.md`
  - [x] In-Database `IsolationForest` anomaly scoring + Basel III traffic-light exception logging
  - [x] Verified persistence in `ANALYTICS.FACT_VAR_ANOMALY_ALERTS`
- [x] **Use Case 2: Systemic Stress Contagion & Liquidity Shock Simulator**
  - [x] Documented in `cortex/2.anomaly_detection/USE_CASE_EXPLANATION.md` & `IMPLEMENTATION_STEPS.md`
  - [x] Multi-round domino cascade propagation algorithm (Direct vs. Secondary Contagion Deficit)
  - [x] Verified persistence in `ANALYTICS.FACT_CONTAGION_SIMULATION_RESULTS`
- [x] **Use Case 3: Counterparty Credit Rating Downgrade Early Warning**
  - [x] Documented in `cortex/3.counterparty_downgrade_predictor/USE_CASE_EXPLANATION.md` & `IMPLEMENTATION_STEPS.md`
  - [x] Modular 3-Stage Architecture:
    - [x] `1_train_downgrade_model.py`: Fits balanced `RandomForestClassifier` on Training Split (`2026-01-01` to `2026-06-30`)
    - [x] `2_validate_downgrade_model.py`: Validation evaluation (AUC: `0.7493`) & automated threshold sweep ($P^* = 0.45$)
    - [x] `3_test_and_score_inference.py`: Out-of-sample holdout test (AUC: `0.7302`) + daily forward scoring (50 counterparties)
  - [x] In-Database Model Registry in `ANALYTICS.FACT_MODEL_STORE`
  - [x] Verified persistence in `ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS`

---

## 🟢 Phase 4: Executive Presentation & AI Delivery
- [x] **Use Case 4: Streamlit in Snowflake (SiS) Risk Command Center Dashboard**
  - [x] Multi-tab interactive dashboard visualizing VaR breaches, systemic cascade graphs, and counterparty downgrade radar (`abor_risk/streamlit/risk_dashboard.py`)
  - [x] Live In-Database Deployment on Snowflake Stage `@ENG.STREAMLIT_STAGE` (`ANALYTICS.RISK_COMMAND_CENTER`)
  - [x] Eisenberg-Noe / Glasserman Multi-Round Contagion Domino Cascade Simulator (`contagion_engine.py`)
- [ ] **Use Case 5: Snowflake Cortex LLM Executive CRO Daily Briefing**
  - [ ] GenAI synthesis of daily risk alerts, breaches, and mitigation recommendations
