"""
DIM_PORTFOLIO Parquet Data Generator (20 Funds Universe)
=========================================================
Generates synthetic portfolio/fund master reference data with dynamic
portfolio manager and strategy migrations across months 202601 through 202609 (9 months).

Covers 20 portfolios spanning key institutional strategy mandates:
  - Fixed Income Long-Only
  - Equity Long/Short & Statistical Arbitrage
  - Global Macro & FX Overlay
  - Credit & Distressed Opportunities
  - Managed Futures & CTAs
  - Multi-Asset Allocation & ESG

Output Path Structure:
  .../abor_risk/outputFile/PORTFOLIO/DATA_YYYYMM/dim_portfolio_YYYYMM.parquet

Usage:
  python generate_dim_portfolio.py --all-months
  python generate_dim_portfolio.py --yyyymm 202605
"""

import os
import argparse
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np


def get_default_last_month() -> str:
    """Calculates last month in YYYYMM format based on current local date."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_date = first_of_this_month - timedelta(days=1)
    return last_month_date.strftime("%Y%m")


def generate_dim_portfolio(yyyymm: str, base_dir: str):
    """
    Generates DIM_PORTFOLIO dataset for the specified YYYYMM month,
    incorporating portfolio master catalog and manager/strategy updates.
    """
    if len(yyyymm) != 6 or not yyyymm.isdigit():
        raise ValueError(f"Invalid YYYYMM format: '{yyyymm}'. Expected 6 digits (e.g. 202609).")

    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])

    # End of target month date
    if month == 12:
        end_of_month_date = date(year, 12, 31)
    else:
        end_of_month_date = date(year, month + 1, 1) - timedelta(days=1)

    # Define Subject Area Output Directory
    subject_area = "PORTFOLIO"
    output_base = os.path.join(base_dir, "outputFile")
    if os.path.isfile(output_base):
        os.remove(output_base)

    output_dir = os.path.join(output_base, subject_area, f"DATA_{yyyymm}")
    os.makedirs(output_dir, exist_ok=True)

    output_file_name = f"dim_portfolio_{yyyymm}.parquet"
    output_file_path = os.path.join(output_dir, output_file_name)

    # Base Portfolio Definitions (20 Institutional Portfolios)
    base_portfolios = {
        "PORT_001": {
            "name": "Global Fixed Income Alpha Fund",
            "base_currency": "USD",
            "initial_manager": "Sarah Jenkins",
            "initial_strategy": "Fixed Income Long-Only",
            "inception_date": date(2018, 3, 15)
        },
        "PORT_002": {
            "name": "US Equity Tactical Hedge Fund",
            "base_currency": "USD",
            "initial_manager": "David Miller",
            "initial_strategy": "Equity Long-Short",
            "inception_date": date(2020, 6, 1)
        },
        "PORT_003": {
            "name": "European Multi-Asset Pension Strategy",
            "base_currency": "EUR",
            "initial_manager": "Jean-Luc Dupont",
            "initial_strategy": "Global Macro",
            "inception_date": date(2019, 11, 20)
        },
        "PORT_004": {
            "name": "Prime Derivatives Arbitrage Portfolio",
            "base_currency": "USD",
            "initial_manager": "Marcus Vance",
            "initial_strategy": "Credit Arbitrage",
            "inception_date": date(2021, 1, 10)
        },
        "PORT_005": {
            "name": "UK Institutional Real Return Fund",
            "base_currency": "GBP",
            "initial_manager": "Oliver Sterling",
            "initial_strategy": "Inflation Linked",
            "inception_date": date(2022, 4, 5)
        },
        "PORT_006": {
            "name": "Global Quantitative Market Neutral",
            "base_currency": "USD",
            "initial_manager": "Elena Rostova",
            "initial_strategy": "Statistical Arbitrage",
            "inception_date": date(2021, 9, 1)
        },
        "PORT_007": {
            "name": "High Yield Corporate Credit Strategy",
            "base_currency": "USD",
            "initial_manager": "Robert Taylor",
            "initial_strategy": "High Yield Credit",
            "inception_date": date(2019, 5, 12)
        },
        "PORT_008": {
            "name": "Emerging Markets Sovereign Debt Fund",
            "base_currency": "USD",
            "initial_manager": "Carlos Mendez",
            "initial_strategy": "Emerging Markets Debt",
            "inception_date": date(2020, 2, 28)
        },
        "PORT_009": {
            "name": "Asia-Pacific Tech Sector Growth Fund",
            "base_currency": "USD",
            "initial_manager": "Kenji Takahashi",
            "initial_strategy": "Sector Growth",
            "inception_date": date(2022, 1, 15)
        },
        "PORT_010": {
            "name": "Nordic Sustainable Climate Leaders",
            "base_currency": "EUR",
            "initial_manager": "Astrid Lindholm",
            "initial_strategy": "ESG Integration",
            "inception_date": date(2021, 11, 1)
        },
        "PORT_011": {
            "name": "Global Currency & Macro Overlay",
            "base_currency": "USD",
            "initial_manager": "Liam O'Connor",
            "initial_strategy": "FX Overlay",
            "inception_date": date(2017, 8, 20)
        },
        "PORT_012": {
            "name": "Private Debt & Direct Lending Vehicle",
            "base_currency": "USD",
            "initial_manager": "Charles Montgomery",
            "initial_strategy": "Direct Lending",
            "inception_date": date(2020, 10, 5)
        },
        "PORT_013": {
            "name": "Swiss Defensive Capital Preservation",
            "base_currency": "CHF",
            "initial_manager": "Beatrix Huber",
            "initial_strategy": "Capital Preservation",
            "inception_date": date(2016, 4, 18)
        },
        "PORT_014": {
            "name": "Tokyo High-Dividend Equity Strategy",
            "base_currency": "JPY",
            "initial_manager": "Haruto Tanaka",
            "initial_strategy": "Dividend Yield",
            "inception_date": date(2018, 12, 10)
        },
        "PORT_015": {
            "name": "North American Energy Transition Fund",
            "base_currency": "USD",
            "initial_manager": "Michael Chang",
            "initial_strategy": "Real Assets",
            "inception_date": date(2022, 7, 1)
        },
        "PORT_016": {
            "name": "Global Convertible Bond Arbitrage",
            "base_currency": "EUR",
            "initial_manager": "Francois Dubois",
            "initial_strategy": "Convertible Arbitrage",
            "inception_date": date(2020, 3, 22)
        },
        "PORT_017": {
            "name": "Systematic Trend Following CTA",
            "base_currency": "USD",
            "initial_manager": "Lars Nielsen",
            "initial_strategy": "Managed Futures",
            "inception_date": date(2019, 1, 14)
        },
        "PORT_018": {
            "name": "Institutional Liquidity & Cash Fund",
            "base_currency": "USD",
            "initial_manager": "Patricia Hayes",
            "initial_strategy": "Cash & Short Duration",
            "inception_date": date(2015, 6, 30)
        },
        "PORT_019": {
            "name": "Cross-Asset Multi-Factor Premia",
            "base_currency": "USD",
            "initial_manager": "Sebastian Becker",
            "initial_strategy": "Multi-Factor",
            "inception_date": date(2021, 4, 12)
        },
        "PORT_020": {
            "name": "Dynamic Global Balanced Allocation",
            "base_currency": "EUR",
            "initial_manager": "Isabella Rossi",
            "initial_strategy": "Balanced Allocation",
            "inception_date": date(2018, 9, 25)
        }
    }

    # Chronological History of Portfolio Attribute Updates (SCD Type 2 Events)
    migration_events = [
        # Event 1 (Feb 2026): PORT_002 Manager reassigned from David Miller -> Vishal Gupta
        {
            "port_id": "PORT_002",
            "event_date": date(2026, 2, 1),
            "new_manager": "Vishal Gupta",
            "new_strategy": None
        },
        # Event 2 (March 2026): PORT_002 Manager reassigned from Vishal Gupta -> Anamika Gupta
        {
            "port_id": "PORT_002",
            "event_date": date(2026, 3, 1),
            "new_manager": "Anamika Gupta",
            "new_strategy": None
        },
        # Event 3 (April 2026): PORT_007 Strategy updated High Yield Credit -> Stressed & Distressed Credit
        {
            "port_id": "PORT_007",
            "event_date": date(2026, 4, 15),
            "new_manager": None,
            "new_strategy": "Stressed & Distressed Credit"
        },
        # Event 4 (May 2026): PORT_011 Manager reassigned Liam O'Connor -> Fiona Gallagher
        {
            "port_id": "PORT_011",
            "event_date": date(2026, 5, 15),
            "new_manager": "Fiona Gallagher",
            "new_strategy": None
        },
        # Event 5 (June 2026): PORT_004 Strategy updated Credit Arbitrage -> Multi-Strategy Arbitrage
        {
            "port_id": "PORT_004",
            "event_date": date(2026, 6, 20),
            "new_manager": None,
            "new_strategy": "Multi-Strategy Arbitrage"
        },
        # Event 6 (Aug 2026): PORT_015 Manager reassigned Michael Chang -> Samantha Wu
        {
            "port_id": "PORT_015",
            "event_date": date(2026, 8, 1),
            "new_manager": "Samantha Wu",
            "new_strategy": None
        }
    ]

    records = []

    # Reconstruct history for each portfolio up to end_of_month_date
    for port_id, meta in base_portfolios.items():
        port_events = [
            e for e in migration_events
            if e["port_id"] == port_id and e["event_date"] <= end_of_month_date
        ]
        port_events = sorted(port_events, key=lambda x: x["event_date"])

        current_start_date = date(2025, 1, 1)
        current_manager = meta["initial_manager"]
        current_strategy = meta["initial_strategy"]

        if not port_events:
            records.append({
                "PORTFOLIO_ID": port_id,
                "PORTFOLIO_NAME": meta["name"],
                "BASE_CURRENCY": meta["base_currency"],
                "PORTFOLIO_MANAGER": current_manager,
                "INVESTMENT_STRATEGY": current_strategy,
                "INCEPTION_DATE": meta["inception_date"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })
        else:
            for ev in port_events:
                records.append({
                    "PORTFOLIO_ID": port_id,
                    "PORTFOLIO_NAME": meta["name"],
                    "BASE_CURRENCY": meta["base_currency"],
                    "PORTFOLIO_MANAGER": current_manager,
                    "INVESTMENT_STRATEGY": current_strategy,
                    "INCEPTION_DATE": meta["inception_date"],
                    "EFFECTIVE_START_DATE": current_start_date,
                    "EFFECTIVE_END_DATE": ev["event_date"] - timedelta(days=1),
                    "IS_ACTIVE": False
                })
                current_start_date = ev["event_date"]
                if ev["new_manager"]:
                    current_manager = ev["new_manager"]
                if ev["new_strategy"]:
                    current_strategy = ev["new_strategy"]

            records.append({
                "PORTFOLIO_ID": port_id,
                "PORTFOLIO_NAME": meta["name"],
                "BASE_CURRENCY": meta["base_currency"],
                "PORTFOLIO_MANAGER": current_manager,
                "INVESTMENT_STRATEGY": current_strategy,
                "INCEPTION_DATE": meta["inception_date"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })

    df = pd.DataFrame(records)
    df = df.sort_values(by=["PORTFOLIO_ID", "EFFECTIVE_START_DATE"]).reset_index(drop=True)

    df.to_parquet(output_file_path, engine="pyarrow", index=False)

    active_cnt = sum(df["IS_ACTIVE"])
    expired_cnt = sum(~df["IS_ACTIVE"])

    print(f"[SUCCESS] Generated DIM_PORTFOLIO Parquet dataset for target month '{yyyymm}'.")
    print(f"   - File Path: {output_file_path}")
    print(f"   - Total Rows: {len(df)} ({active_cnt} Active, {expired_cnt} Historical SCD Type 2)")
    print(f"   - Format: Parquet (PyArrow Engine)")

    return output_file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DIM_PORTFOLIO Parquet Files (202601 to 202609)")
    parser.add_argument(
        "--yyyymm", "-m",
        type=str,
        default=None,
        help="Specific month in YYYYMM format (e.g., 202605)."
    )
    parser.add_argument(
        "--all-months", "-a",
        action="store_true",
        help="Generate all 9 months from 202601 through 202609."
    )
    args = parser.parse_args()

    base_directory = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )

    if args.all_months:
        months_range = [f"20260{m}" for m in range(1, 10)]
        print(f"[START] Generating DIM_PORTFOLIO datasets for all 9 months (202601 to 202609)...\n")
        for m in months_range:
            generate_dim_portfolio(m, base_directory)
        print("\n[COMPLETE] Batch generation finished for all 9 months!")
    else:
        target_month = args.yyyymm if args.yyyymm else get_default_last_month()
        generate_dim_portfolio(target_month, base_directory)
