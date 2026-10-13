# 📈 Analytics Consumption Layer (`ANALYTICS` Schema)
**Module**: `abor_risk/analytics_layer`  
**Target Schema**: `ANALYTICS`

---

## 📌 Overview

The **Analytics Consumption Layer** is the Gold layer of the Medallion architecture. It abstracts the underlying normalized star schema (`ENG.DIM_*`, `ENG.FACT_*`) into ready-to-consume business views tailored for:
* **Chief Risk Officers (CRO) & Executives**: High-level portfolio risk, VaR leverage ratios, and capital allocations.
* **Credit Risk Officers**: Counterparty exposure concentrations against Basel regulatory limits.
* **Portfolio Managers**: Asset class risk attribution and trade execution turnover.
* **Auditors & Compliance**: Credit rating migration audit trails.

---

## 🏗 Views Deployed in Snowflake

| View Name | Source Tables | Key Business Purpose |
| :--- | :--- | :--- |
| **`ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK`** | `FACT_DAILY_POSITIONS_ABOR`, `FACT_DAILY_RISK_METRICS`, `DIM_PORTFOLIO` | Total AUM, 99% VaR, PFE, CVA, Unrealized PnL, and VaR/AUM leverage %. |
| **`ANALYTICS.VW_COUNTERPARTY_CONCENTRATION`** | `FACT_DAILY_RISK_METRICS`, `DIM_COUNTERPARTY` | Basel Large Exposure monitoring (>10% concentration breach warning) and Investment Grade vs Speculative breakdown. |
| **`ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION`** | `FACT_DAILY_RISK_METRICS`, `DIM_INSTRUMENT` | Slices MTM, 99% VaR, and PFE across Equities, Fixed Income, Derivatives, and Currencies. |
| **`ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS`** | `FACT_SETTLED_TRANSACTIONS`, all Dimensions | Measures trading volumes, trade counts, buy/sell ratios, and broker allocation share. |
| **`ANALYTICS.VW_COUNTERPARTY_RATING_MIGRATION`** | `DIM_COUNTERPARTY` (SCD Type 2) | Historical audit trail showing rating transitions (`AAA` $\to$ `BBB`) over time. |

---

## 🚀 Execution & Usage

### 1. Deploy All Analytics Views to Snowflake
```bash
python abor_risk/analytics_layer/deploy_analytics_layer.py --config config/dev.yaml
```

### 2. Query and Display Formatted Terminal Reports
```bash
# Display all reports for a specific snapshot date
python abor_risk/analytics_layer/fetch_analytics_reports.py --date 2026-03-31

# Display only the Executive Portfolio Risk report
python abor_risk/analytics_layer/fetch_analytics_reports.py --report portfolio --date 2026-03-31

# Display only the Counterparty Concentration & Basel Large Exposure report
python abor_risk/analytics_layer/fetch_analytics_reports.py --report cpty --date 2026-03-31

# Display Asset Class breakdown
python abor_risk/analytics_layer/fetch_analytics_reports.py --report asset --date 2026-03-31

# Display Trade Execution & Turnover
python abor_risk/analytics_layer/fetch_analytics_reports.py --report trades
```
