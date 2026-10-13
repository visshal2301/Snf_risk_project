# 📈 Use Case 1: 14-Day Forward Portfolio VaR & AUM Trajectory
**Module**: `abor_risk/cortex/1.var_aum_trajectory`  
**Engine**: Native Snowflake Machine Learning (`SNOWFLAKE.ML.FORECAST`)  
**Target Persona**: Chief Risk Officer (CRO), Portfolio Managers, Investment Committee  

---

## 💡 The Problem in Layman's Terms: Driving with Headlights vs. Rear-View Mirrors

Imagine driving a fast car down a winding mountain road at night:
* **Historical Reporting (Rear-View Mirror)**: Looking only at what is behind you. It tells you: *"Yesterday, we safely navigated that sharp turn."* But it cannot tell you if there is a roadblock around the next bend.
* **Predictive Forecasting (Headlights)**: Beams light 200 meters ahead onto the road. It tells you: *"There is a sharp curve and a steep drop-off coming up in 10 seconds—slow down now."*

In hedge funds and institutional banks, **standard risk systems only look in the rear-view mirror**. Every morning, the risk team calculates what the fund's risk was at yesterday's market close. 

If market conditions become rocky, a fund might be heading toward a massive loss or a regulatory violation next week, but managers will not know until it has already happened.

**Use Case 1 turns on the headlights.** Instead of only calculating yesterday's risk, we use native machine learning to project where each portfolio's risk and assets are heading over the **next 14 trading days**.

---

## 🧭 Key Financial Concepts Explained Simply

### 1. What is AUM (Assets Under Management)?
* **Definition**: The total market value of all investments (stocks, bonds, derivatives, cash) that a fund manages.
* **Example**: If Fund 1 holds \$100M of Apple stock and \$50M of Treasury bonds, its AUM is **\$150 Million**.

### 2. What is 99% VaR (Value-at-Risk)?
* **Definition**: A statistical measure of the **worst-case expected loss** on a bad day under normal market conditions.
* **What "99% VaR = \$2 Million" means**: 
  - On 99 out of 100 trading days, your daily loss will **not exceed \$2 Million**.
  - There is only a 1% chance (a rare, extreme market shock) of losing more than that.
* **Why it matters**: Regulators (like Basel III and the SEC) require funds to hold cash reserves proportional to their VaR.

### 3. What is the VaR-to-AUM Leverage Ratio?
* **Formula**: $\frac{\text{VaR}}{\text{AUM}} \times 100$
* **Why it matters**: If a \$100M fund has a \$5M VaR, its risk ratio is **5%** (conservative). If its VaR rises to \$20M, its ratio is **20%** (dangerously leveraged).

---

## 🎯 What Does This ML Model Do?

Using Snowflake’s automated time-series engine (`SNOWFLAKE.ML.FORECAST`):

1. **Learns Past Behavior**: The model analyzes our 195 consecutive trading days (January through September 2026) for each of our 20 investment portfolios. It learns their trends, volatility patterns, and day-of-week momentum.
2. **Projects 14 Days Ahead**: For every single portfolio, it projects daily numbers from **Day +1 to Day +14**:
   - **Expected Value (Middle Line)**: The most likely trajectory.
   - **Upper Bound (Worst-Case Band)**: A 95% confidence upper limit.
   - **Lower Bound (Best-Case Band)**: A 95% confidence lower limit.
3. **Automated Risk Limit Warning**:
   - Every portfolio has a board-mandated risk limit (for example, VaR must not exceed 15% of AUM).
   - If the model’s 14-day forecast shows that a fund's VaR is projected to cross that threshold next week, the system flags an **Early Breach Warning**.

---

## 🏢 Real-World Business Value

| Who Benefits? | How It Helps Them | Real-World Action Taken |
| :--- | :--- | :--- |
| **Portfolio Manager** | Advance warning before limits are breached | Sells risky assets or buys hedge options *today*, avoiding forced fire-sales next week. |
| **Chief Risk Officer (CRO)** | Forward-looking capital planning | Allocates extra cash reserves to portfolios entering turbulent market regimes. |
| **Compliance & Auditors** | Proof of proactive governance | Demonstrates to regulatory bodies (SEC, FINRA, Basel) that the firm maintains predictive, automated risk controls. |

---

## 📊 Visualizing the Output

When you view the forecast, each portfolio's trajectory looks like this:

```text
  Value-at-Risk ($)
      ▲
      │                                   ┌─ Upper Bound (Worst Case: $2.4M) [!] LIMIT BREACH
      │                              . - '
      │                         . - ' ─── Expected Forecast ($2.1M)
      │                    . - '
      │       ────────────' ───────────── Lower Bound (Best Case: $1.8M)
      │      (Historical)       (14-Day Forward Forecast Window)
      └─────────────────────────────────────────────────────────────────► Time (Days)
        Jan 2026        Sep 30      Oct 1       Oct 7       Oct 14
```

* **If the Upper Bound touches or crosses the red limit line**: The risk desk receives an immediate action item to trim exposure.
