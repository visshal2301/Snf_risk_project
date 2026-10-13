"""
Snowflake Cortex AI Executive Risk Intelligence Layer
======================================================
Queries quantitative risk metrics from ENG.FACT_DAILY_RISK_METRICS and dimension metadata,
invoking Snowflake Cortex LLM (SNOWFLAKE.CORTEX.COMPLETE) to generate executive-ready risk briefings.
Includes graceful fallback handling for Snowflake trial account restrictions.

Usage:
  python abor_risk/cortex/generate_risk_briefing.py --date 2026-03-31
  python abor_risk/cortex/generate_risk_briefing.py --all-dates
"""

import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.connection.session import SnowflakeSessionManager
from abor_risk.utils.audit_logger import AuditLogger

logger = get_logger("CortexExecutiveBriefing", log_file="abor_ingestion.log")


def fetch_risk_summary_data(session, metric_date: str):
    """
    Fetches aggregated portfolio risk metrics and top counterparty exposures for a specific snapshot date.
    """
    query = f"""
    SELECT 
        r.METRIC_DATE,
        p.PORTFOLIO_NAME,
        p.PORTFOLIO_MANAGER,
        p.INVESTMENT_STRATEGY,
        c.COUNTERPARTY_NAME,
        c.CREDIT_RATING,
        i.INSTRUMENT_NAME,
        i.ASSET_CLASS,
        r.MARK_TO_MARKET,
        r.VALUE_AT_RISK_99,
        r.POTENTIAL_FUTURE_EXPOSURE,
        r.CREDIT_VALUE_ADJUSTMENT
    FROM ENG.FACT_DAILY_RISK_METRICS r
    JOIN ENG.DIM_PORTFOLIO p ON r.PORTFOLIO_ID = p.PORTFOLIO_ID AND p.IS_ACTIVE = TRUE
    JOIN ENG.DIM_COUNTERPARTY c ON r.COUNTERPARTY_ID = c.COUNTERPARTY_ID AND c.IS_ACTIVE = TRUE
    JOIN ENG.DIM_INSTRUMENT i ON r.INSTRUMENT_ID = i.INSTRUMENT_ID AND i.IS_ACTIVE = TRUE
    WHERE r.METRIC_DATE = '{metric_date}'
    ORDER BY r.MARK_TO_MARKET DESC;
    """
    return session.sql(query).collect()


def generate_fallback_executive_briefing(metric_date: str, rows) -> str:
    """
    Analytical rule-based briefing synthesizer used when Cortex LLM is disabled on trial accounts.
    """
    if not rows:
        return f"No risk metric data found for snapshot date {metric_date}."

    total_mtm = sum(r["MARK_TO_MARKET"] for r in rows)
    total_var_99 = sum(r["VALUE_AT_RISK_99"] for r in rows)
    total_pfe = sum(r["POTENTIAL_FUTURE_EXPOSURE"] for r in rows)
    total_cva = sum(r["CREDIT_VALUE_ADJUSTMENT"] for r in rows)

    # Top high risk exposures
    high_risk_rows = [r for r in rows if r["CREDIT_RATING"] in ('B', 'CCC', 'BB', 'OTHER')]
    top_counterparty = rows[0]["COUNTERPARTY_NAME"] if rows else "N/A"
    top_mtm = rows[0]["MARK_TO_MARKET"] if rows else 0.0

    var_pct = (total_var_99 / abs(total_mtm) * 100) if total_mtm != 0 else 0.0

    briefing = f"""
================================================================================
EXECUTIVE RISK BRIEFING REPORT (CORTEX AI SYNTHESIS)
Snapshot Date: {metric_date}
Generated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
================================================================================

1. EXECUTIVE SUMMARY
---------------------
- Total Portfolio Mark-to-Market : ${total_mtm:,.2f}
- Value at Risk (99% VaR)        : ${total_var_99:,.2f} ({var_pct:.2f}% of total MTM)
- Potential Future Exposure (PFE): ${total_pfe:,.2f}
- Credit Value Adjustment (CVA)  : ${total_cva:,.2f}
- Evaluated Risk Positions       : {len(rows)} positions

2. KEY RISK HIGHLIGHTS & COUNTERPARTY CONCENTRATION
--------------------------------------------------
- Largest Counterparty Position : {top_counterparty} (${top_mtm:,.2f} MTM)
- High-Risk Credit Band Count   : {len(high_risk_rows)} positions in speculative rating bands (BB or lower).
- Risk Engine Driver            : Stored Procedure ENG.SP_CALCULATE_DAILY_RISK_METRICS & UDF ENG.FN_GET_CREDIT_RISK_WEIGHT.

3. RECOMMENDATIONS FOR CHIEF RISK OFFICER (CRO)
-----------------------------------------------
- Monitor counterparties in lower credit rating tiers with elevated CVA impact.
- Maintain adequate liquidity buffer against 99% VaR tail exposure (${total_var_99:,.2f}).
- Re-align collateral requirements for non-investment grade positions.

================================================================================
    """.strip()
    return briefing


def generate_cortex_risk_briefing(session, metric_date: str):
    """
    Generates an executive briefing using Snowflake Cortex AI COMPLETE function, with automatic fallback.
    """
    logger.info("==========================================================================")
    logger.info(f"STARTING CORTEX AI EXECUTIVE RISK BRIEFING | Snapshot Date: '{metric_date}'")
    logger.info("==========================================================================")

    yyyymm = metric_date.replace("-", "")[:6]
    audit_logger = AuditLogger(session, "CortexRiskBriefing", "EXECUTIVE_BRIEFING", yyyymm)

    try:
        rows = fetch_risk_summary_data(session, metric_date)
        if not rows:
            logger.warning(f"No risk metric records found for date '{metric_date}'. Skipping briefing generation.")
            audit_logger.log_success(staged_rows=0, loaded_rows=0, expired_rows=0)
            return

        total_mtm = sum(r["MARK_TO_MARKET"] for r in rows)
        total_var = sum(r["VALUE_AT_RISK_99"] for r in rows)

        prompt_text = (
            f"You are a Chief Risk Officer (CRO) at a global investment firm. "
            f"Analyze the following portfolio risk metrics for date {metric_date}:\n"
            f"- Total Mark-to-Market: ${total_mtm:,.2f}\n"
            f"- Total 99% VaR: ${total_var:,.2f}\n"
            f"- Top Position Count: {len(rows)}\n\n"
            f"Provide a 3-paragraph executive briefing highlighting: 1) Portfolio Exposure & VaR, "
            f"2) Credit Risk Concentration, 3) Actionable Mitigation Recommendations."
        )

        cortex_sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            '{prompt_text.replace("'", "''")}'
        ) AS EXECUTIVE_BRIEFING;
        """

        try:
            logger.info("Calling SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b')...")
            res = session.sql(cortex_sql).collect()
            briefing_text = res[0]["EXECUTIVE_BRIEFING"]
            logger.info("[CORTEX LLM SUCCESS] Native Cortex AI Briefing successfully generated!")
        except Exception as cortex_err:
            logger.warning(f"[CORTEX FALLBACK] Native Cortex LLM unavailable: {cortex_err}")
            logger.info("Switching to Analytical Rule-Based Executive Synthesis Engine...")
            briefing_text = generate_fallback_executive_briefing(metric_date, rows)

        print("\n" + briefing_text + "\n")

        audit_logger.log_success(staged_rows=0, loaded_rows=len(rows), expired_rows=0)

        logger.info("==========================================================================")
        logger.info(f"CORTEX AI BRIEFING COMPLETED SUCCESSFULLY | Snapshot Date: '{metric_date}'")
        logger.info(f"  - Audit Log Entry : ENG.AUDIT_EXECUTION_LOG (LOG_ID: {audit_logger.log_id})")
        logger.info("==========================================================================\n")

    except Exception as e:
        logger.error(f"[CORTEX BRIEFING FAILED] Date: {metric_date} | Error: {str(e)}", exc_info=True)
        if 'audit_logger' in locals() and audit_logger:
            audit_logger.log_failure(str(e))
        raise e


def main():
    parser = argparse.ArgumentParser(description="Generate Executive Risk Briefing via Snowflake Cortex AI")
    parser.add_argument("--date", "-d", type=str, default=None, help="Snapshot date (e.g., 2026-03-31)")
    parser.add_argument("--all-dates", action="store_true", help="Generate briefings across all position dates")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file")
    args = parser.parse_args()

    config = load_config(args.config)
    session = SnowflakeSessionManager(config).get_session()

    try:
        session.sql(f"USE DATABASE {config['snowflake']['database']}").collect()
        session.sql("USE SCHEMA ENG").collect()

        if args.all_dates:
            dates_df = session.table("ENG.FACT_DAILY_RISK_METRICS").select("METRIC_DATE").distinct().order_by("METRIC_DATE").collect()
            date_list = [str(row[0]) for row in dates_df]
            logger.info(f"Generating Executive Briefings across {len(date_list)} snapshot dates: {date_list}")
            for target_date in date_list:
                generate_cortex_risk_briefing(session, target_date)
        else:
            target_date = args.date or "2026-03-31"
            generate_cortex_risk_briefing(session, target_date)

    except Exception as e:
        logger.error(f"[CORTEX PIPELINE FAILED] Exception: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
