"""
DIM_INSTRUMENT Parquet Data Generator
======================================
Generates synthetic financial instrument master reference data with dynamic
multi-asset taxonomy across months 202601 through 202609 (9 months).

Output Path Structure:
  .../abor_risk/outputFile/INSTRUMENT/DATA_YYYYMM/dim_instrument_YYYYMM.parquet

Usage:
  python generate_dim_instrument.py --all-months
  python generate_dim_instrument.py --yyyymm 202605
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


def generate_dim_instrument(yyyymm: str, base_dir: str):
    """
    Generates DIM_INSTRUMENT dataset for the specified YYYYMM month,
    incorporating instrument master catalog and taxonomy updates.
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
    subject_area = "INSTRUMENT"
    output_base = os.path.join(base_dir, "outputFile")
    if os.path.isfile(output_base):
        os.remove(output_base)

    output_dir = os.path.join(output_base, subject_area, f"DATA_{yyyymm}")
    os.makedirs(output_dir, exist_ok=True)

    output_file_name = f"dim_instrument_{yyyymm}.parquet"
    output_file_path = os.path.join(output_dir, output_file_name)

    # Base Multi-Asset Instruments Catalog (Initial state as of 2025-01-01) - 60 Master Instruments
    base_instruments = {
        # =========================================================================
        # 1. FIXED INCOME (20 Instruments)
        # =========================================================================
        "INST_101": {
            "name": "US Treasury 10Y Bond 4.25%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "USD",
            "maturity_date": date(2034, 8, 15)
        },
        "INST_105": {
            "name": "Tech Corp 5.50% Senior Notes",
            "asset_class": "Fixed Income",
            "sub_asset_class": "High Yield Corporate",
            "currency": "USD",
            "maturity_date": date(2030, 5, 1)
        },
        "INST_106": {
            "name": "UK Gilt 10Y Bond 3.75%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "GBP",
            "maturity_date": date(2035, 1, 15)
        },
        "INST_110": {
            "name": "Germany Bund 10Y 2.50%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "EUR",
            "maturity_date": date(2034, 10, 15)
        },
        "INST_113": {
            "name": "US Treasury 2Y Note 4.75%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "USD",
            "maturity_date": date(2026, 9, 30)
        },
        "INST_114": {
            "name": "US Treasury 5Y Note 4.10%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "USD",
            "maturity_date": date(2029, 5, 31)
        },
        "INST_115": {
            "name": "US Treasury 30Y Bond 4.50%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "USD",
            "maturity_date": date(2054, 2, 15)
        },
        "INST_116": {
            "name": "French OAT 10Y 3.00%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "EUR",
            "maturity_date": date(2034, 5, 25)
        },
        "INST_117": {
            "name": "Japan Government 10Y Bond 0.95%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "JPY",
            "maturity_date": date(2034, 6, 20)
        },
        "INST_118": {
            "name": "Italy BTP 10Y 3.85%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "EUR",
            "maturity_date": date(2034, 9, 1)
        },
        "INST_119": {
            "name": "UK Gilt 5Y Bond 3.50%",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Government Bond",
            "currency": "GBP",
            "maturity_date": date(2029, 10, 22)
        },
        "INST_120": {
            "name": "Apple Inc 3.85% Senior Notes 2032",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Investment Grade Corporate",
            "currency": "USD",
            "maturity_date": date(2032, 5, 4)
        },
        "INST_121": {
            "name": "Microsoft Corp 3.30% Notes 2031",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Investment Grade Corporate",
            "currency": "USD",
            "maturity_date": date(2031, 2, 6)
        },
        "INST_122": {
            "name": "JPMorgan Chase 4.35% Fixed-to-Float 2030",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Investment Grade Corporate",
            "currency": "USD",
            "maturity_date": date(2030, 4, 22)
        },
        "INST_123": {
            "name": "BNP Paribas 3.875% Subordinated 2032",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Investment Grade Corporate",
            "currency": "EUR",
            "maturity_date": date(2032, 1, 10)
        },
        "INST_124": {
            "name": "Shell International Finance 3.25% 2028",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Investment Grade Corporate",
            "currency": "USD",
            "maturity_date": date(2028, 4, 6)
        },
        "INST_125": {
            "name": "Delta Air Lines 7.375% Senior Notes 2026",
            "asset_class": "Fixed Income",
            "sub_asset_class": "High Yield Corporate",
            "currency": "USD",
            "maturity_date": date(2026, 11, 15)
        },
        "INST_126": {
            "name": "Petrobras Global Finance 6.85% 2031",
            "asset_class": "Fixed Income",
            "sub_asset_class": "High Yield Corporate",
            "currency": "USD",
            "maturity_date": date(2031, 6, 5)
        },
        "INST_127": {
            "name": "Telecom Italia 5.30% Senior Notes 2027",
            "asset_class": "Fixed Income",
            "sub_asset_class": "High Yield Corporate",
            "currency": "EUR",
            "maturity_date": date(2027, 2, 14)
        },
        "INST_128": {
            "name": "Green Energy Transition Bond 4.75% 2033",
            "asset_class": "Fixed Income",
            "sub_asset_class": "Emerging Markets Bond",
            "currency": "EUR",
            "maturity_date": date(2033, 3, 31)
        },

        # =========================================================================
        # 2. EQUITIES & EQUITY ETFS (20 Instruments)
        # =========================================================================
        "INST_104": {
            "name": "Apple Inc Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_108": {
            "name": "Microsoft Corp Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_109": {
            "name": "iShares High Yield Corp Bond ETF",
            "asset_class": "Equities",
            "sub_asset_class": "Fixed Income ETF",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_112": {
            "name": "Amazon.com Inc Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_129": {
            "name": "Nvidia Corp Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_130": {
            "name": "Alphabet Inc Class A",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_131": {
            "name": "Meta Platforms Inc",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_132": {
            "name": "Tesla Inc Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_133": {
            "name": "JPMorgan Chase & Co Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_134": {
            "name": "Exxon Mobil Corp Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_135": {
            "name": "Eli Lilly and Co Common Stock",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_136": {
            "name": "ASML Holding NV",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "EUR",
            "maturity_date": None
        },
        "INST_137": {
            "name": "LVMH Moet Hennessy Louis Vuitton",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "EUR",
            "maturity_date": None
        },
        "INST_138": {
            "name": "AstraZeneca PLC",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "GBP",
            "maturity_date": None
        },
        "INST_139": {
            "name": "Shell PLC",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "GBP",
            "maturity_date": None
        },
        "INST_140": {
            "name": "TotalEnergies SE",
            "asset_class": "Equities",
            "sub_asset_class": "Large Cap Equity",
            "currency": "EUR",
            "maturity_date": None
        },
        "INST_141": {
            "name": "SPDR S&P 500 ETF Trust",
            "asset_class": "Equities",
            "sub_asset_class": "Index ETF",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_142": {
            "name": "Invesco QQQ Trust Series 1",
            "asset_class": "Equities",
            "sub_asset_class": "Index ETF",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_143": {
            "name": "iShares MSCI Emerging Markets ETF",
            "asset_class": "Equities",
            "sub_asset_class": "Index ETF",
            "currency": "USD",
            "maturity_date": None
        },
        "INST_144": {
            "name": "Vanguard Total Bond Market ETF",
            "asset_class": "Equities",
            "sub_asset_class": "Fixed Income ETF",
            "currency": "USD",
            "maturity_date": None
        },

        # =========================================================================
        # 3. DERIVATIVES (12 Instruments)
        # =========================================================================
        "INST_102": {
            "name": "S&P 500 Index Equity Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Equity Swap",
            "currency": "USD",
            "maturity_date": date(2027, 12, 31)
        },
        "INST_111": {
            "name": "Gold Spot Commodity Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Commodity Swap",
            "currency": "USD",
            "maturity_date": date(2027, 6, 30)
        },
        "INST_145": {
            "name": "Euro Stoxx 50 Total Return Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Equity Swap",
            "currency": "EUR",
            "maturity_date": date(2027, 6, 30)
        },
        "INST_146": {
            "name": "Nasdaq 100 Index Equity Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Equity Swap",
            "currency": "USD",
            "maturity_date": date(2028, 3, 31)
        },
        "INST_147": {
            "name": "USD SOFR 5Y Interest Rate Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Interest Rate Swap",
            "currency": "USD",
            "maturity_date": date(2029, 12, 31)
        },
        "INST_148": {
            "name": "EUR EURIBOR 10Y Interest Rate Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Interest Rate Swap",
            "currency": "EUR",
            "maturity_date": date(2034, 12, 31)
        },
        "INST_149": {
            "name": "CDX North American Investment Grade CDS",
            "asset_class": "Derivatives",
            "sub_asset_class": "Credit Default Swap",
            "currency": "USD",
            "maturity_date": date(2029, 6, 20)
        },
        "INST_150": {
            "name": "iTraxx Europe Crossover High Yield CDS",
            "asset_class": "Derivatives",
            "sub_asset_class": "Credit Default Swap",
            "currency": "EUR",
            "maturity_date": date(2029, 6, 20)
        },
        "INST_151": {
            "name": "US Treasury 10Y Note Futures Dec 26",
            "asset_class": "Derivatives",
            "sub_asset_class": "Bond Futures",
            "currency": "USD",
            "maturity_date": date(2026, 12, 21)
        },
        "INST_152": {
            "name": "Brent Crude Oil Commodity Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Commodity Swap",
            "currency": "USD",
            "maturity_date": date(2027, 3, 31)
        },
        "INST_153": {
            "name": "Natural Gas Financial Swap",
            "asset_class": "Derivatives",
            "sub_asset_class": "Commodity Swap",
            "currency": "USD",
            "maturity_date": date(2026, 12, 31)
        },
        "INST_154": {
            "name": "Copper Grade A Commodity Forward",
            "asset_class": "Derivatives",
            "sub_asset_class": "Commodity Swap",
            "currency": "USD",
            "maturity_date": date(2027, 9, 30)
        },

        # =========================================================================
        # 4. FOREIGN EXCHANGE (8 Instruments)
        # =========================================================================
        "INST_103": {
            "name": "EUR/USD Foreign Exchange Forward",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Forward",
            "currency": "EUR",
            "maturity_date": date(2026, 12, 15)
        },
        "INST_107": {
            "name": "GBP/USD FX Spot Option",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Option",
            "currency": "GBP",
            "maturity_date": date(2026, 11, 30)
        },
        "INST_155": {
            "name": "USD/JPY FX Forward 6M",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Forward",
            "currency": "JPY",
            "maturity_date": date(2026, 10, 31)
        },
        "INST_156": {
            "name": "USD/CHF Cross Currency Swap",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "Cross Currency Swap",
            "currency": "CHF",
            "maturity_date": date(2028, 5, 31)
        },
        "INST_157": {
            "name": "AUD/USD FX Forward",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Forward",
            "currency": "USD",
            "maturity_date": date(2026, 12, 31)
        },
        "INST_158": {
            "name": "EUR/GBP FX Option",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Option",
            "currency": "EUR",
            "maturity_date": date(2026, 9, 30)
        },
        "INST_159": {
            "name": "USD/CAD FX Spot Forward",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "FX Forward",
            "currency": "USD",
            "maturity_date": date(2026, 11, 15)
        },
        "INST_160": {
            "name": "EUR/JPY Cross Currency Basis Swap",
            "asset_class": "Foreign Exchange",
            "sub_asset_class": "Cross Currency Swap",
            "currency": "EUR",
            "maturity_date": date(2029, 3, 31)
        }
    }

    # Chronological History of Instrument Taxonomy Updates (SCD Type 2 Events)
    migration_events = [
        # Event 1 (Mar 2026): INST_105 upgraded sub_asset_class to Investment Grade Corporate
        {
            "inst_id": "INST_105",
            "event_date": date(2026, 3, 1),
            "new_sub_asset_class": "Investment Grade Corporate"
        },
        # Event 2 (May 2026): INST_128 reclassified to ESG Green Corporate
        {
            "inst_id": "INST_128",
            "event_date": date(2026, 5, 10),
            "new_sub_asset_class": "ESG Green Corporate"
        },
        # Event 3 (Jul 2026): INST_109 reclassified sub_asset_class to High Yield ETF
        {
            "inst_id": "INST_109",
            "event_date": date(2026, 7, 15),
            "new_sub_asset_class": "High Yield ETF"
        },
        # Event 4 (Aug 2026): INST_149 rolled over benchmark series
        {
            "inst_id": "INST_149",
            "event_date": date(2026, 8, 20),
            "new_sub_asset_class": "Credit Default Swap Series 42"
        }
    ]

    records = []

    # Reconstruct history for each instrument up to end_of_month_date
    for inst_id, meta in base_instruments.items():
        inst_events = [
            e for e in migration_events
            if e["inst_id"] == inst_id and e["event_date"] <= end_of_month_date
        ]
        inst_events = sorted(inst_events, key=lambda x: x["event_date"])

        current_start_date = date(2025, 1, 1)
        current_sub_asset = meta["sub_asset_class"]

        if not inst_events:
            records.append({
                "INSTRUMENT_ID": inst_id,
                "INSTRUMENT_NAME": meta["name"],
                "ASSET_CLASS": meta["asset_class"],
                "SUB_ASSET_CLASS": current_sub_asset,
                "CURRENCY": meta["currency"],
                "MATURITY_DATE": meta["maturity_date"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })
        else:
            for ev in inst_events:
                records.append({
                    "INSTRUMENT_ID": inst_id,
                    "INSTRUMENT_NAME": meta["name"],
                    "ASSET_CLASS": meta["asset_class"],
                    "SUB_ASSET_CLASS": current_sub_asset,
                    "CURRENCY": meta["currency"],
                    "MATURITY_DATE": meta["maturity_date"],
                    "EFFECTIVE_START_DATE": current_start_date,
                    "EFFECTIVE_END_DATE": ev["event_date"] - timedelta(days=1),
                    "IS_ACTIVE": False
                })
                current_start_date = ev["event_date"]
                current_sub_asset = ev["new_sub_asset_class"]

            records.append({
                "INSTRUMENT_ID": inst_id,
                "INSTRUMENT_NAME": meta["name"],
                "ASSET_CLASS": meta["asset_class"],
                "SUB_ASSET_CLASS": current_sub_asset,
                "CURRENCY": meta["currency"],
                "MATURITY_DATE": meta["maturity_date"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })

    df = pd.DataFrame(records)
    df = df.sort_values(by=["INSTRUMENT_ID", "EFFECTIVE_START_DATE"]).reset_index(drop=True)

    df.to_parquet(output_file_path, engine="pyarrow", index=False)

    active_cnt = sum(df["IS_ACTIVE"])
    expired_cnt = sum(~df["IS_ACTIVE"])

    print(f"[SUCCESS] Generated DIM_INSTRUMENT Parquet dataset for target month '{yyyymm}'.")
    print(f"   - File Path: {output_file_path}")
    print(f"   - Total Rows: {len(df)} ({active_cnt} Active, {expired_cnt} Historical SCD Type 2)")
    print(f"   - Format: Parquet (PyArrow Engine)")

    return output_file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DIM_INSTRUMENT Parquet Files (202601 to 202609)")
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
        print(f"[START] Generating DIM_INSTRUMENT datasets for all 9 months (202601 to 202609)...\n")
        for m in months_range:
            generate_dim_instrument(m, base_directory)
        print("\n[COMPLETE] Batch generation finished for all 9 months!")
    else:
        target_month = args.yyyymm if args.yyyymm else get_default_last_month()
        generate_dim_instrument(target_month, base_directory)
