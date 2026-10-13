# 🚨 Use Case 2: Real-Time Portfolio Risk & Asset Anomaly Detection
**Module**: `abor_risk/cortex/2.anomaly_detection`  
**Engine**: Native Snowflake Machine Learning (`SNOWFLAKE.ML.ANOMALY_DETECTION`)  
**Target Persona**: Chief Risk Officer (CRO), Trade Compliance / Surveillance Desk, Portfolio Managers, Middle-Office Risk Operations  

---

## 💡 The Problem in Layman's Terms: Thermometers vs. Smoke Detectors

Imagine running a high-security institutional building:
* **The Thermometer (Traditional Risk Reporting)**: Every morning at 8:00 AM, a technician reads the thermometer and writes: *"The room is 72°F."* It tells you the ambient temperature once a day. But if a fire starts at 2:00 PM, the thermometer will not wake anyone up until tomorrow morning's report—by which time the room has burned down.
* **The Smoke Detector (Dynamic Anomaly Detection)**: Constantly monitors the air. The moment an unexpected cloud of smoke billows—even if the overall room temperature hasn't risen yet—it sounds an immediate high-decibel alarm: *"Zone 4 Anomaly Detected—Investigate Now!"*

In investment banks and hedge funds, **95% of risk management still operates like a thermometer**. Daily VaR is calculated overnight. If a trader makes a catastrophic "fat-finger" typo or an OTC pricing feed sends corrupted numbers during the trading session, risk teams often don't catch it until the next day's batch run.

**Use Case 2 builds an intelligent, automated smoke detector directly inside Snowflake.**

---

## 🌪️ Real-World Catastrophic Risk Scenarios We Catch

### 1. The "Fat-Finger" Execution Error
* **What happens**: A junior trader or algorithmic broker intends to enter an order for **10,000 contracts** of S&P futures, but accidentally inputs an extra zero: **100,000 contracts**.
* **The Impact**: In our real dataset, on **June 18, 2026**, **Portfolio 004 (Prime Derivatives Arbitrage Portfolio)** experienced a sudden jump where its 99% VaR exploded from **$2.5 Million to $17.5 Million** in a single snapshot, driving leverage to an alarming **26.77%**.
* **How ML Catches It**: The anomaly detector flags this instant deviation as an **Extreme Outlier (Percentile > 0.999)** within seconds of data ingestion.

### 2. Sudden AUM Drainage ("The Silent Run")
* **What happens**: A large institutional client (e.g., a sovereign wealth fund) exercises a sudden withdrawal of \$50M across 48 hours without prior notice to the risk desk.
* **The Impact**: While the fund's absolute risk dollar amount didn't change, its capital cushion evaporated. The portfolio's VaR-to-AUM leverage ratio suddenly doubles, putting the firm in violation of regulatory capital limits.
* **How ML Catches It**: By learning normal day-to-day cash flow patterns, the model flags abnormal asset contraction before margin calls occur.

### 3. Stale or Corrupted Market Data Feeds
* **What happens**: An upstream pricing vendor (e.g., for illiquid convertible bonds or credit default swaps) sends a quote with a displaced decimal point (e.g., \$100.00 instead of \$10.00).
* **The Impact**: Collateral valuations and margin requirements calculated by accounting (ABOR) are completely falsified.
* **How ML Catches It**: Detects that the asset valuation deviates wildly from historical volatility bounds and marks `IS_ANOMALY = TRUE`.

---

## 🧠 Why Static Rules Fail vs. Dynamic Machine Learning

Most legacy risk systems use **static threshold rules**, for example: *"Alert if daily VaR moves by more than \$2 Million."*

### Why Static Thresholds Fail:
1. **The Size Asymmetry Problem**:
   - A \$2M jump in a **\$500 Million Global Balanced Fund** is a tiny, normal 0.4% fluctuation (causes false alarms).
   - A \$2M jump in a **\$15 Million Micro-Cap Strategy** represents a catastrophic 13.3% shock (the static rule might miss it if the threshold is set too high).
2. **Day-of-Week Seasonality**:
   - Trading volumes and risk naturally contract on Friday afternoons and expand on Monday mornings. Static rules ignore this calendar DNA.

### How Native Snowflake Anomaly Detection Solves It:
`SNOWFLAKE.ML.ANOMALY_DETECTION` trains dynamically on each portfolio's specific time-series:
* **Tailored Volatility Envelopes**: Each portfolio gets its own customized dynamic boundary (`LOWER_BOUND` to `UPPER_BOUND`).
* **Directional Context**: Distinguishes between normal volatility spikes (e.g., expected Fed interest rate announcement days) and true statistical shocks.
* **Confidence Tuning**: Generates an exact mathematical score:
  - `IS_ANOMALY`: Boolean (`TRUE` / `FALSE`)
  - `PERCENTILE`: How extreme the deviation is (e.g., 0.998 = 99.8th percentile rarity)
  - `DISTANCE`: Absolute dollar distance from the expected equilibrium value

---

## 🏢 Business Value & ROI by Persona

| Persona | Core Pain Point | How Use Case 2 Helps Them | Real-World Financial Impact |
| :--- | :--- | :--- | :--- |
| **Chief Risk Officer (CRO)** | Blind spots between daily batch risk reports | Instant alerting on extreme risk expansion | Prevents rogue trading blowups like Knight Capital (\$440M lost in 45 minutes). |
| **Trade Surveillance & Compliance** | Drowning in thousands of false-positive alerts | Dynamic ML filters out normal market noise | Focuses investigation only on high-confidence ($>99\%$) genuine anomalies. |
| **Portfolio Managers** | Unaware of pricing feed glitches distorting their book | Flags corrupted inputs before executing hedge trades | Eliminates costly trades made on corrupted market quotes. |
| **Middle-Office Operations** | Manual spreadsheet reconciliation | Automated Gold table: `FACT_RISK_ANOMALIES` | Fast audit trail for regulatory inquiries (SEC, FINRA, Basel III). |

---

## 🎯 What We Will Build Next

1. **Scaffolding Data Pipeline**: Dense daily time series for anomaly model training across all 20 portfolios.
2. **In-Snowflake Model Training**: `CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ANALYTICS.ML_RISK_ANOMALY_DETECTOR`.
3. **Automated Anomaly Scoring**: Running `CALL model!DETECT_ANOMALIES` to flag extreme events (including the historic June 18 spike in `PORT_004`).
4. **Persistence Layer**: Storing flagged events into `ANALYTICS.FACT_RISK_ANOMALIES`.
5. **Surveillance Alert Dashboard**: Formatted executive report highlighting confirmed anomalies, severity ratings, and recommended investigation steps.
