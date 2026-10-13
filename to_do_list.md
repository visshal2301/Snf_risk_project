# 📋 Project To-Do List & Backlog Tracker
**Project**: Institutional ABOR & Quantitative Risk Platform (`abor_risk`)  
**Status Date**: October 2026  

---

## ⏸️ Parked Items
| ID | Item | Description | Status / Reason |
| :--- | :--- | :--- | :--- |
| **PARK-01** | **Snowflake Cortex AI Integration** | `SNOWFLAKE.CORTEX.COMPLETE` LLM executive briefing (`abor_risk/cortex/generate_risk_briefing.py` and `VW_EXECUTIVE_RISK_REPORT`). | **PARKED** — Cortex functions are unavailable or restricted on current Snowflake trial account / region. Rule-based briefing fallback exists. |

---

## 🟢 Completed: Analytics Consumption Layer (`ANALYTICS` Schema)
| ID | Feature / View | Target View | Status |
| :--- | :--- | :--- | :--- |
| **ANALYTICS-01** | **Executive Portfolio Risk View** | `ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK` | ✅ **LIVE & VALIDATED** (15 rows) |
| **ANALYTICS-02** | **Counterparty Concentration & Basel Large Exposure Mart** | `ANALYTICS.VW_COUNTERPARTY_CONCENTRATION` | ✅ **LIVE & VALIDATED** (29 rows) |
| **ANALYTICS-03** | **Asset Class & Tenor Risk Mart** | `ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION` | ✅ **LIVE & VALIDATED** (29 rows) |
| **ANALYTICS-04** | **Trade Execution & Liquidity Turnover Mart** | `ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS` | ✅ **LIVE & VALIDATED** (343 rows) |
| **ANALYTICS-05** | **SCD Type 2 Credit Rating Migration Audit View** | `ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION` | ✅ **LIVE & VALIDATED** (11 rows) |


---

## 🟢 Completed: Cortex Machine Learning & Risk Intelligence Suite
| ID | Model / Pipeline | Target Schema / Module | Status |
| :--- | :--- | :--- | :--- |
| **CORTEX-01** | **VaR Anomaly & Backtesting Breach Detector** | `ANALYTICS.FACT_VAR_ANOMALY_ALERTS` | ✅ **LIVE & VALIDATED** (`IsolationForest`) |
| **CORTEX-02** | **Systemic Contagion & Liquidity Shock Simulator** | `ANALYTICS.FACT_CONTAGION_SIMULATION_RESULTS` | ✅ **LIVE & VALIDATED** (Cascade Rounds) |
| **CORTEX-03** | **Counterparty Downgrade Early Warning (Train/Val/Score)** | `ANALYTICS.FACT_COUNTERPARTY_DOWNGRADE_PREDICTIONS` | ✅ **LIVE & VALIDATED** (Balanced RF, $P^*=0.45$) |
| **CORTEX-04** | **Streamlit in Snowflake (SiS) Risk Command Center** | `ANALYTICS.RISK_COMMAND_CENTER` | ✅ **LIVE & DEPLOYED** (`abor_risk/streamlit/`) |

---

## 🚀 Secondary Backlog
| ID | Feature / Task | Target File / Area | Details & Requirements |
| :--- | :--- | :--- | :--- |
| **TODO-01** | **Date Range Backfill Capability** | `abor_risk/main.py` | Add CLI support for `--from-yyyymm` and `--to-yyyymm` (e.g. `202601` to `202609`) to automatically iterate 9 months. |
| **TODO-02** | **Orphan Key Alert Thresholding** | `abor_risk/ingestion/factTable/` | Parameterize the orphan key tolerance threshold (warn vs abort) in configuration. |
| **TODO-03** | **Create `ENG.PIPELINE_ERROR_LOG` DDL** | `sql/migrations/` | Create table DDL for `ENG.PIPELINE_ERROR_LOG` so stored procedure exception handler does not fail. |
| **TODO-04** | **Automated Schema Migration Runner** | `scripts/run_migrations.py` | Create a Python/Snowpark runner to apply versioned migrations (`V1.0.0` through `V6.0.0`) in sequence. |
| **TODO-05** | **Automated Test Suite for `abor_risk`** | `tests/unit/` | JSON contracts verified via pytest. Integration tests ready. |

---

## ❓ Pending & Unclear Tasks (Requiring Clarification)

### 1. Dual Risk Engines: CSV Factor Simulation vs. ABOR Table Risk
* **Resolution**:
  * **Engine A (CSV prototype)**: Archived into `archive/legacy_poc/`.
  * **Engine B (`abor_risk/riskEngine/`)**: Established as the sole, official institutional ABOR risk platform running in Snowflake.

---

### 2. General Ledger / `DIM_CHART_OF_ACCOUNTS` Scope
* **Current State**:
  * Mentioned in the Medallion Architecture diagram (`project_flow.md` line 46: `DIM_CHART_OF_ACCOUNTS`), but no DDL, synthetic data generator, JSON contract, or ingestion pipeline exists.
* **Question / Gap**:
  * Is Chart of Accounts (GL taxonomy) required for this phase, or was it a conceptual roadmap item to remain in the backlog?

---

### 3. User Step "Point 5"
* **Context**:
  * In earlier discussions: *"Once we are done with 3 [Audit Execution Logging] we can start with 5."*
* **Question / Gap**:
  * What specifically did you define as **Point 5**? (e.g., Downstream reporting/views, integration testing, end-to-end backfill orchestration, or deployment packaging?)

---

### 4. Orphan Key Policy (Warn vs. Hard Abort)
* **Current State**:
  * Missing foreign keys in `FACT_DAILY_POSITIONS_ABOR` and `FACT_DAILY_TRANSACTIONS_ABOR` currently log a console/file warning and increment `WARNING_COUNT` in `AUDIT_EXECUTION_LOG`, but proceed with insertion.
* **Question / Gap**:
  * What exact threshold triggers a **warning** vs a **hard abort/rollback** (e.g., >5% orphan keys or fixed count > 50)?
