# 🏛️ Institutional ABOR & Quantitative Risk Analytics Platform on Snowflake

A production-grade, enterprise financial risk platform built natively on **Snowflake Virtual Warehouses**, **Snowpark Python**, **SCD Type 2 Data Modeling**, and **Cortex Machine Learning / Graph Contagion Simulation**.

---

## 📌 Executive Architecture & Overview

This platform processes daily **Accounting Book of Record (ABOR)** feeds, enforces strict zero-lookahead temporal integrity via Slowly Changing Dimensions (SCD Type 2), calculates portfolio credit and market risk metrics in-database, and powers an advanced suite of **Cortex AI & ML Risk Use Cases**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        ENTERPRISE ABOR & RISK DATA FLOW                                │
└────────────────────────────────────────────────────────────────────────────────────────┘

  [ BRONZE LAYER ]       [ SILVER LAYER (ENG Schema) ]           [ GOLD LAYER (ANALYTICS) ]
 ┌─────────────────┐    ┌───────────────────────────────┐       ┌───────────────────────────┐
 │ ABOR Parquet    │───►│ SCD2 Dimensions:              │──────►│ Executive Risk Marts:     │
 │ - POSITIONS     │    │   DIM_COUNTERPARTY (SCD2)     │       │   VW_EXECUTIVE_PORTFOLIO  │
 │ - TRANSACTIONS  │    │   DIM_PORTFOLIO, DIM_INST     │       │   VW_CPTY_CONCENTRATION   │
 │ - COUNTERPARTY  │    │ Fact Ingestion:               │       │   VW_ASSET_CLASS_RISK     │
 │ - INSTRUMENT    │    │   FACT_DAILY_POSITIONS_ABOR   │       │   VW_TRADE_TURNOVER       │
 │ - PORTFOLIO     │    │   FACT_SETTLED_TRANSACTIONS   │       └───────────────────────────┘
 └─────────────────┘    │ Risk Engine SPROC:            │                     │
                        │   FACT_DAILY_RISK_METRICS     │                     ▼
                        │   (VaR 99%, PFE 95%, CVA)     │       [ AI / CORTEX RISK SUITE ]
                        └───────────────────────────────┘       ┌───────────────────────────┐
                                        │                       │ 1. VaR Anomaly Detector   │
                                        ▼                       │ 2. Contagion Simulator    │
                        ┌───────────────────────────────┐       │ 3. Downgrade Predictor    │
                        │ ENG.AUDIT_EXECUTION_LOG       │       │ 4. SiS Command Center     │
                        │ Two-Tier Enterprise Audit     │       │ 5. AI CRO GenAI Briefing  │
                        └───────────────────────────────┘       └───────────────────────────┘
```

---

## 🧠 The 3 Cortex AI / ML Quantitative Risk Modules

The platform includes three machine learning models operating directly within Snowflake virtual warehouses:

### 1. 📈 Time-Series VaR & AUM Trajectory Forecasting
* **Module**: [`abor_risk/cortex/1.var_aum_trajectory/`](file:///d:/Padai/My_Project/Snowflake_Risk_project/abor_risk/cortex/1.var_aum_trajectory/)
* **Engine**: Snowpark Python + `SNOWFLAKE.ML.FORECAST`
* **Function**: Generates 14-day forward-looking 99% VaR and Mark-to-Market trajectories with 95% confidence intervals, enabling proactive treasury liquidity buffering.

### 2. 🚨 Native Risk & Anomaly Detection Engine
* **Module**: [`abor_risk/cortex/2.anomaly_detection/`](file:///d:/Padai/My_Project/Snowflake_Risk_project/abor_risk/cortex/2.anomaly_detection/)
* **Engine**: Snowpark Python + `SNOWFLAKE.ML.ANOMALY_DETECTION`
* **Function**: Flags anomalous trade volume spikes and abrupt shifts in counterparty concentration exceeding statistical operating bands.

### 3. 📉 Counterparty Credit Rating Downgrade Early Warning
* **Module**: [`abor_risk/cortex/3.counterparty_downgrade_predictor/`](file:///d:/Padai/My_Project/Snowflake_Risk_project/abor_risk/cortex/3.counterparty_downgrade_predictor/)
* **Engine**: Modular 3-Stage Pipeline (Train, Validate, Score) + Balanced `RandomForestClassifier`
* **Function**: Predicts 30-day forward credit rating migrations ($P \ge 0.45$) using rolling MTM volatility, Basel concentration, and rating tenure; maps counterparties into a 4-Tier Operational Action Matrix.

---

## 📁 Repository Directory Structure

```text
Snowflake_Risk_project/
├── .env                                # Snowflake credentials & environment secrets
├── pyproject.toml                      # Build tool configurations
├── requirements.txt                    # Project dependencies (Snowpark, scikit-learn, etc.)
│
├── abor_risk/                          # 🏛️ PRIMARY PRODUCTION PLATFORM
│   ├── ingestion/                      # Medallion Silver ingestion (SCD2 Dimensions & Fact ABOR)
│   ├── riskEngine/                     # Snowflake Stored Procedure Risk Engine (VaR, PFE, CVA)
│   ├── analytics_layer/                # Gold layer reporting views & analytical marts
│   ├── cortex/                         # 🧠 Native AI/ML & Quantitative Risk Engines
│   │   ├── 1.var_aum_trajectory/      # Use Case 1: Time-Series VaR & AUM Trajectory Forecasting
│   │   ├── 2.anomaly_detection/        # Use Case 2: Native Risk & Anomaly Detection Engine
│   │   └── 3.counterparty_downgrade_predictor/ # Use Case 3: Downgrade Predictor (Train/Val/Score)
│   ├── streamlit/                      # 📊 Streamlit in Snowflake (SiS) Risk Command Center
│   │   ├── risk_dashboard.py           # Multi-tab executive risk dashboard & cascade simulator
│   │   ├── contagion_engine.py         # Eisenberg-Noe domino cascade shock simulation model
│   │   ├── data_service.py             # Snowflake session manager & cached query service
│   │   └── deploy_sis.py               # Automated deployment to Snowflake Stage (@ENG.STREAMLIT_STAGE)
│   ├── fileGeneration/                 # Synthetic ABOR Parquet generator (Multi-month feeds)
│   ├── metadata/                       # JSON Schema Contracts & Data Governance definitions
│   ├── outputFile/                     # Generated Parquet ABOR data files
│   ├── utils/                          # Audit logger & platform telemetry
│   └── main.py                         # Master ingestion & pipeline orchestrator
│
├── src/                                # 🔌 Shared Core Infrastructure
│   ├── connection/                     # Snowflake Snowpark SessionManager
│   └── utils/                          # YAML config loaders & system logging
│
├── sql/                                # Versioned DDL Migrations
│   └── migrations/                     # Table definitions & schema evolution
│
├── scripts/                            # Operational utility runners
│   ├── init_db.py                      # Database initializer
│   └── run_migration.py                # Migration runner
│
├── tests/                              # Automated test suite
│   ├── unit/                           # Schema contract & logic unit tests
│   └── integration/                    # Live Snowflake connectivity tests
│
└── archive/                            # 📦 Archived Assets
    └── legacy_poc/                     # Archived initial CSV prototype (Engine A)
```

---

## 🚀 Quickstart & Pipeline Execution

### 1. Ingest ABOR Feeds & Maintain SCD Type 2 Dimensions
```bash
# Ingest single month of ABOR positions, transactions, and SCD2 dimensions
python abor_risk/main.py --year 2026 --month 1
```

### 2. Execute In-Database Daily Risk Engine SPROC
```bash
# Calculates 99% VaR, 95% PFE, and CVA across all counterparties for a date
python abor_risk/riskEngine/calculate_risk_metrics.py --date 2026-09-25
```

### 3. Run Cortex AI / ML Risk Engines

```bash
# 1. Use Case 1: Time-Series VaR & AUM Trajectory Forecast (SNOWFLAKE.ML.FORECAST)
python abor_risk/cortex/1.var_aum_trajectory/run_var_forecast_pipeline.py

# 2. Use Case 2: Native Anomaly Detection Engine (SNOWFLAKE.ML.ANOMALY_DETECTION)
python abor_risk/cortex/2.anomaly_detection/run_anomaly_detection_pipeline.py

# 3. Use Case 3: Counterparty Downgrade Early Warning (RandomForest + Model Store)
python abor_risk/cortex/3.counterparty_downgrade_predictor/run_counterparty_downgrade_pipeline.py

# Or run Use Case 3 stages independently:
python abor_risk/cortex/3.counterparty_downgrade_predictor/1_train_downgrade_model.py
python abor_risk/cortex/3.counterparty_downgrade_predictor/2_validate_downgrade_model.py
python abor_risk/cortex/3.counterparty_downgrade_predictor/3_test_and_score_inference.py
```

### 4. Launch Streamlit Risk Command Center (SiS)
```bash
# Option A: Run Locally via Streamlit
streamlit run abor_risk/streamlit/risk_dashboard.py

# Option B: Deploy Natively into Snowflake (Streamlit in Snowflake - SiS)
python abor_risk/streamlit/deploy_sis.py
# (Or deploy via SnowSQL: snowsql -f abor_risk/streamlit/deploy_sis.sql)
```

### 5. Run Test Suite
```bash
pytest tests/unit
```

---

## 🛡️ Enterprise Governance & Two-Tier Audit Logging
Every pipeline run registers comprehensive telemetry:
1. **Local Disk**: `logs/abor_ingestion.log`
2. **Snowflake Table**: `ENG.AUDIT_EXECUTION_LOG` (Tracking `PIPELINE_NAME`, `LOADED_ROW_COUNT`, `WARNING_COUNT`, `STATUS`, and `EXECUTION_DURATION_SEC`).
