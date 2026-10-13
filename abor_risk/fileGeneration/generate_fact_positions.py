"""
FACT_DAILY_POSITIONS_ABOR Parquet Data Generator
==================================================
Generates synthetic ABOR daily positions snapshot data for specified dates or months.
Scaled to institutional volume: 20 Portfolios, 50 Counterparties, 60 Instruments.
Supports daily business day snapshots across 202601 through 202609 (~30,000 records).

Includes Injected Ground-Truth Anomaly 3:
  - 2026-06-18: Concentration Blowout ($52,000,000 position in PORT_004 with CPTY_013).

Output Paths:
  Daily Mode  : .../abor_risk/outputFile/POSITIONS/DATA_YYYYMM/POSITIONS_DAILY_YYYYMMDD.parquet
  Monthly Mode: .../abor_risk/outputFile/POSITIONS/DATA_YYYYMM/POSITIONS_MONTHLY_YYYYMM.parquet

Usage:
  python abor_risk/fileGeneration/generate_fact_positions.py --yyyymmdd 20260315
  python abor_risk/fileGeneration/generate_fact_positions.py --yyyymm 202603
  python abor_risk/fileGeneration/generate_fact_positions.py --all-months
"""

import os
import sys
import argparse
from datetime import datetime, date, timedelta
import math
import pandas as pd
import numpy as np


def get_default_last_month() -> str:
    """Calculates last month in YYYYMM format based on current local date."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_date = first_of_this_month - timedelta(days=1)
    return last_month_date.strftime("%Y%m")


# ---------------------------------------------------------------------------
# Institutional Portfolio Holding Model (Stable across time for continuity)
# ---------------------------------------------------------------------------
def _build_portfolio_master_holdings():
    """
    Defines stable holding mandates for 20 portfolios across 60 instruments
    and 50 counterparties, ensuring realistic asset allocation continuity.
    """
    holdings = []
    # 20 Portfolios x 8 Holdings each = 160 core positions
    for p_idx in range(1, 21):
        port_id = f"PORT_{p_idx:03d}"
        for h_idx in range(8):
            # Select instrument deterministically based on portfolio mandate
            inst_num = 101 + ((p_idx * 3 + h_idx * 7) % 60)
            inst_id = f"INST_{inst_num}"

            # Select preferred counterparty
            cpty_num = 1 + ((p_idx * 5 + h_idx * 11) % 50)
            cpty_id = f"CPTY_{cpty_num:03d}"

            # Specific binding for PORT_004 to hold CPTY_009 on INST_125 (Target for Anomaly 3)
            if port_id == "PORT_004" and h_idx == 0:
                inst_id = "INST_125"
                cpty_id = "CPTY_009"

            # Base parameters
            base_qty = float((((p_idx * 7 + h_idx * 13) % 20) + 10) * 1000)  # 10,000 to 30,000 units
            base_cost = 95.0 + float((inst_num % 15))  # $95 to $110 per unit

            holdings.append({
                "PORTFOLIO_ID": port_id,
                "INSTRUMENT_ID": inst_id,
                "COUNTERPARTY_ID": cpty_id,
                "BASE_QTY": base_qty,
                "BASE_COST": base_cost,
                "VOLATILITY": 0.008 + float((inst_num % 5)) * 0.003  # 0.8% to 2.0% daily vol
            })
    return holdings


_PORTFOLIO_HOLDINGS = _build_portfolio_master_holdings()


def _get_business_days_in_month(year: int, month: int):
    """Returns list of business days (Monday to Friday) in target year and month."""
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year, 12, 31)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)

    cur = start_date
    bdays = []
    while cur <= end_date:
        if cur.weekday() < 5:  # Mon-Fri
            bdays.append(cur)
        cur += timedelta(days=1)
    return bdays


def generate_positions_for_date(target_date: date, seed_offset: int = 0) -> pd.DataFrame:
    """
    Generates synthetic positions snapshot for a specific date, modeling
    continuous price drift, daily market volatility, and injected anomalies.
    """
    # Deterministic seed based on day offset from 2026-01-01
    epoch_date = date(2026, 1, 1)
    day_diff = (target_date - epoch_date).days
    np.random.seed(int(target_date.strftime("%Y%m%d")) + seed_offset)

    records = []

    for h in _PORTFOLIO_HOLDINGS:
        port_id = h["PORTFOLIO_ID"]
        inst_id = h["INSTRUMENT_ID"]
        cpty_id = h["COUNTERPARTY_ID"]
        base_qty = h["BASE_QTY"]
        base_cost = h["BASE_COST"]
        daily_vol = h["VOLATILITY"]

        # Deterministic cumulative price factor using sine trends + pseudo-random noise
        # This gives continuous time-series properties without drift divergence
        trend = 1.0 + 0.05 * math.sin(day_diff / 45.0 + int(inst_id[5:]) * 0.2)
        daily_noise = float(np.random.normal(0.0, daily_vol))
        current_price = base_cost * trend * (1.0 + daily_noise)

        # Quantity has minor daily balance shifts (+/- 2%)
        qty_noise = float(np.random.uniform(0.98, 1.02))
        settled_qty = round(base_qty * qty_noise, 0)

        book_val = round(settled_qty * base_cost, 2)
        market_val = round(settled_qty * current_price, 2)
        unrealized_pnl = round(market_val - book_val, 2)

        # -------------------------------------------------------------------
        # INJECTED GROUND-TRUTH ANOMALY 3: Concentration Blowout
        # Date: 2026-06-18 | Target: PORT_004 & CPTY_009 & INST_125
        # -------------------------------------------------------------------
        if target_date == date(2026, 6, 18) and port_id == "PORT_004" and cpty_id == "CPTY_009":
            settled_qty = 520000.0
            book_val = 48000000.00
            market_val = 52000000.00  # $52M unhedged concentration blowout
            unrealized_pnl = 4000000.00

        records.append({
            "POSITION_DATE": target_date,
            "PORTFOLIO_ID": port_id,
            "COUNTERPARTY_ID": cpty_id,
            "INSTRUMENT_ID": inst_id,
            "SETTLED_QUANTITY": settled_qty,
            "BOOK_VALUE": book_val,
            "MARKET_VALUE": market_val,
            "UNREALIZED_PNL": unrealized_pnl
        })

    df = pd.DataFrame(records)
    return df


def generate_fact_positions(yyyymm: str = None, yyyymmdd: str = None, base_dir: str = None):
    """
    Generates position Parquet datasets in daily or monthly partition folders.
    Monthly mode generates daily business day snapshots for the entire month.
    """
    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    output_base = os.path.join(base_dir, "outputFile", "POSITIONS")

    if yyyymmdd:
        # Daily Mode
        target_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        month_str = target_date.strftime("%Y%m")
        output_dir = os.path.join(output_base, f"DATA_{month_str}")
        os.makedirs(output_dir, exist_ok=True)

        file_name = f"POSITIONS_DAILY_{yyyymmdd}.parquet"
        file_path = os.path.join(output_dir, file_name)

        df = generate_positions_for_date(target_date)
        df.to_parquet(file_path, index=False)
        print(f"[POSITIONS] Daily snapshot generated: {file_path} ({len(df)} rows)")
        return file_path

    else:
        # Monthly Batch Mode (All Business Days in Month)
        if not yyyymm:
            yyyymm = get_default_last_month()

        year = int(yyyymm[:4])
        month = int(yyyymm[4:6])

        bdays = _get_business_days_in_month(year, month)
        output_dir = os.path.join(output_base, f"DATA_{yyyymm}")
        os.makedirs(output_dir, exist_ok=True)

        file_name = f"POSITIONS_MONTHLY_{yyyymm}.parquet"
        file_path = os.path.join(output_dir, file_name)

        dfs = []
        for d in bdays:
            dfs.append(generate_positions_for_date(d))

        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_parquet(file_path, index=False)
        print(f"[POSITIONS] Monthly dataset generated: {file_path}")
        print(f"   - Total Business Days: {len(bdays)} ({bdays[0]} to {bdays[-1]})")
        print(f"   - Total Rows: {len(combined_df)} ({len(combined_df) // len(bdays)} rows/day across 20 portfolios)")
        return file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ABOR Positions Parquet Dataset")
    parser.add_argument("--yyyymm", "-m", type=str, help="Target month in YYYYMM format (e.g. 202603)")
    parser.add_argument("--yyyymmdd", "-d", type=str, help="Target date in YYYYMMDD format (e.g. 20260315)")
    parser.add_argument("--all-months", action="store_true", help="Generate monthly datasets for 202601 to 202609")
    args = parser.parse_args()

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    if args.all_months:
        print("[START] Generating FACT_DAILY_POSITIONS_ABOR datasets for all 9 months (202601 to 202609)...\n")
        total_rows = 0
        for m in [f"20260{i}" for i in range(1, 10)]:
            fp = generate_fact_positions(yyyymm=m, base_dir=project_root)
            df_check = pd.read_parquet(fp)
            total_rows += len(df_check)
        print(f"\n[COMPLETE] Batch positions generation finished! Cumulative rows across 9 months: {total_rows}")
    elif args.yyyymmdd:
        generate_fact_positions(yyyymmdd=args.yyyymmdd, base_dir=project_root)
    else:
        month_to_run = args.yyyymm or get_default_last_month()
        generate_fact_positions(yyyymm=month_to_run, base_dir=project_root)
