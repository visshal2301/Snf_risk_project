"""
Analytics Reports Query & Display Tool
=======================================
Queries the business views in ANALYTICS schema and renders formatted tabular reports:
  1. Executive Portfolio Risk & Return Report
  2. Counterparty Credit Concentration & Basel Large Exposure Report
  3. Asset Class Exposure & Risk Distribution Report
  4. Trade Execution & Broker Turnover Report

Usage:
  python abor_risk/analytics_layer/fetch_analytics_reports.py --date 2026-03-31
  python abor_risk/analytics_layer/fetch_analytics_reports.py --all-dates
  python abor_risk/analytics_layer/fetch_analytics_reports.py --report portfolio --date 2026-03-31
  python abor_risk/analytics_layer/fetch_analytics_reports.py --report cpty --date 2026-03-31
"""

import sys
import os
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

logger = get_logger("AnalyticsReports", log_file="abor_ingestion.log")


def print_portfolio_risk_report(session, target_date: str = None):
    """Prints Executive Portfolio Risk Summary."""
    date_filter = f"WHERE SNAPSHOT_DATE = '{target_date}'" if target_date else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        PORTFOLIO_NAME,
        PORTFOLIO_MANAGER,
        INVESTMENT_STRATEGY,
        ACTIVE_HOLDINGS_COUNT AS HOLDINGS,
        TOTAL_AUM_USD,
        TOTAL_UNREALIZED_PNL_USD,
        TOTAL_VAR_99_USD,
        TOTAL_PFE_95_USD,
        TOTAL_CVA_USD,
        VAR_TO_AUM_PCT
    FROM ANALYTICS.VW_EXECUTIVE_PORTFOLIO_RISK
    {date_filter}
    ORDER BY SNAPSHOT_DATE DESC, TOTAL_AUM_USD DESC;
    """
    rows = session.sql(sql).collect()

    print("\n" + "=" * 125)
    print(f"[REPORT 1] EXECUTIVE PORTFOLIO RISK REPORT {f'| Snapshot Date: {target_date}' if target_date else '| All Dates'}")
    print("=" * 125)
    header = (
        f"{'DATE':<10} | {'PORTFOLIO':<24} | {'MANAGER':<18} | {'HOLDINGS':<8} | "
        f"{'AUM ($)':<14} | {'UNREAL PNL ($)':<14} | {'99% VaR ($)':<12} | {'PFE ($)':<12} | {'CVA ($)':<10} | {'VaR/AUM %'}"
    )
    print(header)
    print("-" * 125)
    for r in rows:
        aum_str = f"${r['TOTAL_AUM_USD']:,.2f}" if r['TOTAL_AUM_USD'] is not None else "$0.00"
        pnl_str = f"${r['TOTAL_UNREALIZED_PNL_USD']:,.2f}" if r['TOTAL_UNREALIZED_PNL_USD'] is not None else "$0.00"
        var_str = f"${r['TOTAL_VAR_99_USD']:,.2f}" if r['TOTAL_VAR_99_USD'] is not None else "$0.00"
        pfe_str = f"${r['TOTAL_PFE_95_USD']:,.2f}" if r['TOTAL_PFE_95_USD'] is not None else "$0.00"
        cva_str = f"${r['TOTAL_CVA_USD']:,.2f}" if r['TOTAL_CVA_USD'] is not None else "$0.00"
        pct_str = f"{r['VAR_TO_AUM_PCT']:.2f}%" if r['VAR_TO_AUM_PCT'] is not None else "0.00%"

        print(
            f"{str(r['SNAPSHOT_DATE']):<10} | {r['PORTFOLIO_NAME']:<24} | {r['PORTFOLIO_MANAGER']:<18} | "
            f"{r['HOLDINGS']:<8} | {aum_str:<14} | {pnl_str:<14} | {var_str:<12} | {pfe_str:<12} | {cva_str:<10} | {pct_str}"
        )
    print("=" * 125 + "\n")


def print_counterparty_concentration_report(session, target_date: str = None):
    """Prints Counterparty Concentration & Basel Large Exposure Summary."""
    date_filter = f"WHERE SNAPSHOT_DATE = '{target_date}'" if target_date else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        COUNTERPARTY_NAME,
        SECTOR,
        CREDIT_RATING,
        RATING_CATEGORY,
        NET_EXPOSURE_USD,
        VAR_99_USD,
        PFE_95_USD,
        CVA_PROVISION_USD,
        FIRM_CONCENTRATION_PCT,
        BASEL_CONCENTRATION_STATUS
    FROM ANALYTICS.VW_COUNTERPARTY_CONCENTRATION
    {date_filter}
    ORDER BY SNAPSHOT_DATE DESC, NET_EXPOSURE_USD DESC;
    """
    rows = session.sql(sql).collect()

    print("\n" + "=" * 135)
    print(f"[REPORT 2] COUNTERPARTY CONCENTRATION & BASEL LARGE EXPOSURE REPORT {f'| Snapshot Date: {target_date}' if target_date else ''}")
    print("=" * 135)
    header = (
        f"{'DATE':<10} | {'COUNTERPARTY':<26} | {'SECTOR':<16} | {'RATING':<6} | "
        f"{'EXPOSURE ($)':<14} | {'99% VaR ($)':<12} | {'PFE ($)':<12} | {'CVA ($)':<10} | {'CONC %':<8} | {'STATUS'}"
    )
    print(header)
    print("-" * 135)
    for r in rows:
        exp_str = f"${r['NET_EXPOSURE_USD']:,.2f}" if r['NET_EXPOSURE_USD'] is not None else "$0.00"
        var_str = f"${r['VAR_99_USD']:,.2f}" if r['VAR_99_USD'] is not None else "$0.00"
        pfe_str = f"${r['PFE_95_USD']:,.2f}" if r['PFE_95_USD'] is not None else "$0.00"
        cva_str = f"${r['CVA_PROVISION_USD']:,.2f}" if r['CVA_PROVISION_USD'] is not None else "$0.00"
        conc_str = f"{r['FIRM_CONCENTRATION_PCT']:.2f}%" if r['FIRM_CONCENTRATION_PCT'] is not None else "0.00%"

        status_flag = r['BASEL_CONCENTRATION_STATUS']
        if status_flag == "BREACH_WARNING":
            status_display = f"[!] {status_flag}"
        elif status_flag == "ELEVATED":
            status_display = f"[*] {status_flag}"
        else:
            status_display = f"[OK] {status_flag}"

        print(
            f"{str(r['SNAPSHOT_DATE']):<10} | {r['COUNTERPARTY_NAME']:<26} | {r['SECTOR']:<16} | {r['CREDIT_RATING']:<6} | "
            f"{exp_str:<14} | {var_str:<12} | {pfe_str:<12} | {cva_str:<10} | {conc_str:<8} | {status_display}"
        )
    print("=" * 135 + "\n")


def print_asset_class_report(session, target_date: str = None):
    """Prints Asset Class & Taxonomy Risk Distribution Summary."""
    date_filter = f"WHERE SNAPSHOT_DATE = '{target_date}'" if target_date else ""
    sql = f"""
    SELECT 
        SNAPSHOT_DATE,
        ASSET_CLASS,
        SUB_ASSET_CLASS,
        CURRENCY,
        INSTRUMENTS_HELD_COUNT,
        TOTAL_MARKET_VALUE_USD,
        TOTAL_VAR_99_USD,
        TOTAL_PFE_95_USD,
        TOTAL_CVA_USD
    FROM ANALYTICS.VW_ASSET_CLASS_RISK_DISTRIBUTION
    {date_filter}
    ORDER BY SNAPSHOT_DATE DESC, TOTAL_MARKET_VALUE_USD DESC;
    """
    rows = session.sql(sql).collect()

    print("\n" + "=" * 115)
    print(f"[REPORT 3] ASSET CLASS & CURRENCY RISK DISTRIBUTION REPORT {f'| Snapshot Date: {target_date}' if target_date else ''}")
    print("=" * 115)
    header = (
        f"{'DATE':<10} | {'ASSET CLASS':<18} | {'SUB-ASSET CLASS':<22} | {'CCY':<4} | "
        f"{'COUNT':<6} | {'MTM VALUE ($)':<16} | {'99% VaR ($)':<14} | {'CVA ($)'}"
    )
    print(header)
    print("-" * 115)
    for r in rows:
        mtm_str = f"${r['TOTAL_MARKET_VALUE_USD']:,.2f}" if r['TOTAL_MARKET_VALUE_USD'] is not None else "$0.00"
        var_str = f"${r['TOTAL_VAR_99_USD']:,.2f}" if r['TOTAL_VAR_99_USD'] is not None else "$0.00"
        cva_str = f"${r['TOTAL_CVA_USD']:,.2f}" if r['TOTAL_CVA_USD'] is not None else "$0.00"

        print(
            f"{str(r['SNAPSHOT_DATE']):<10} | {r['ASSET_CLASS']:<18} | {r['SUB_ASSET_CLASS']:<22} | {r['CURRENCY']:<4} | "
            f"{r['INSTRUMENTS_HELD_COUNT']:<6} | {mtm_str:<16} | {var_str:<14} | {cva_str}"
        )
    print("=" * 115 + "\n")


def print_trade_turnover_report(session):
    """Prints Settled Trade Turnover & Broker Allocation Summary."""
    sql = """
    SELECT 
        TRANSACTION_DATE,
        PORTFOLIO_NAME,
        BROKER_NAME,
        BROKER_RATING,
        ASSET_CLASS,
        TRANSACTION_TYPE,
        TOTAL_TRADES_COUNT,
        TOTAL_UNITS_TRADED,
        TOTAL_VOLUME_USD
    FROM ANALYTICS.VW_TRADE_TURNOVER_ANALYTICS
    ORDER BY TRANSACTION_DATE DESC, TOTAL_VOLUME_USD DESC
    LIMIT 15;
    """
    rows = session.sql(sql).collect()

    print("\n" + "=" * 115)
    print("[REPORT 4] TRADE TURNOVER & BROKER EXECUTION REPORT (Top Recent Batches)")
    print("=" * 115)
    header = (
        f"{'TXN DATE':<10} | {'PORTFOLIO':<24} | {'BROKER':<22} | {'RATING':<6} | "
        f"{'ASSET CLASS':<14} | {'SIDE':<5} | {'TRADES':<6} | {'VOLUME ($)'}"
    )
    print(header)
    print("-" * 115)
    for r in rows:
        vol_str = f"${r['TOTAL_VOLUME_USD']:,.2f}" if r['TOTAL_VOLUME_USD'] is not None else "$0.00"
        print(
            f"{str(r['TRANSACTION_DATE']):<10} | {r['PORTFOLIO_NAME']:<24} | {r['BROKER_NAME']:<22} | {r['BROKER_RATING']:<6} | "
            f"{r['ASSET_CLASS']:<14} | {r['TRANSACTION_TYPE']:<5} | {r['TOTAL_TRADES_COUNT']:<6} | {vol_str}"
        )
    print("=" * 115 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Fetch and Display Analytics Layer Reports")
    parser.add_argument("--date", "-d", type=str, default="2026-03-31", help="Snapshot date (e.g., 2026-03-31)")
    parser.add_argument("--all-dates", action="store_true", help="Display metrics across all available dates")
    parser.add_argument("--report", "-r", type=str, default="all", choices=["all", "portfolio", "cpty", "asset", "trades"],
                        help="Specific report to display (default: all)")
    parser.add_argument("--config", "-c", type=str, default="config/dev.yaml", help="Path to config file")
    args = parser.parse_args()

    target_date = None if args.all_dates else args.date

    config = load_config(args.config)
    session_mgr = SnowflakeSessionManager(config)
    session = session_mgr.get_session()

    try:
        if args.report in ("all", "portfolio"):
            print_portfolio_risk_report(session, target_date)
        if args.report in ("all", "cpty"):
            print_counterparty_concentration_report(session, target_date)
        if args.report in ("all", "asset"):
            print_asset_class_report(session, target_date)
        if args.report in ("all", "trades"):
            print_trade_turnover_report(session)
    finally:
        session.close()


if __name__ == "__main__":
    main()

