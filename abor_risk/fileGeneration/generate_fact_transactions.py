"""
FACT_SETTLED_TRANSACTIONS Parquet Data Generator
=================================================
Generates synthetic ABOR settled transactions trade execution data.
Scaled to institutional volume: 20 Portfolios, 50 Counterparties, 60 Instruments.
Supports daily trading activity across months 202601 through 202609 (~4,000 trades).

Includes Injected Ground-Truth Anomalies:
  - Anomaly 1 (2026-04-14): Fat-Finger Trade ($45,000,000 settled amount).
  - Anomaly 2 (2026-05-22): Flash-Pricing Error (unit price = $12.50, 88% crash).
  - Anomaly 4 (2026-07-29): Runaway Algo Burst (95 rapid-fire trades from PORT_003).

Output Paths:
  Daily Mode  : .../abor_risk/outputFile/TRANSACTIONS/DATA_YYYYMM/TRANSACTIONS_DAILY_YYYYMMDD.parquet
  Monthly Mode: .../abor_risk/outputFile/TRANSACTIONS/DATA_YYYYMM/TRANSACTIONS_MONTHLY_YYYYMM.parquet

Usage:
  python abor_risk/fileGeneration/generate_fact_transactions.py --yyyymmdd 20260315
  python abor_risk/fileGeneration/generate_fact_transactions.py --yyyymm 202603
  python abor_risk/fileGeneration/generate_fact_transactions.py --all-months
"""

import os
import sys
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


def generate_transactions_for_range(start_date: date, end_date: date, seed_offset: int = 0) -> pd.DataFrame:
    """
    Generates synthetic settled transactions between start_date and end_date,
    sampling across 20 portfolios, 50 counterparties, and 60 instruments.
    Injects ground-truth anomalies 1, 2, and 4 on designated business days.
    """
    np.random.seed(int(start_date.strftime("%Y%m%d")) + seed_offset)

    portfolios = [f"PORT_{i:03d}" for i in range(1, 21)]
    counterparties = [f"CPTY_{i:03d}" for i in range(1, 51)]
    instruments = [f"INST_{i}" for i in range(101, 161)]
    txn_types = ["BUY", "SELL", "REPO", "REVERSE_REPO"]
    currencies = ["USD", "EUR", "GBP"]

    records = []
    current_date = start_date
    txn_seq = 1000

    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday to Friday
            # Generate 18-24 normal trades per business day
            num_trades = np.random.randint(18, 25)
            for _ in range(num_trades):
                txn_seq += 1
                txn_id = f"TXN_{current_date.strftime('%Y%m%d')}_{txn_seq}"
                
                # T+2 settlement (skip weekend if settlement lands on Sat/Sun)
                settle_date = current_date + timedelta(days=2)
                if settle_date.weekday() == 5:
                    settle_date += timedelta(days=2)
                elif settle_date.weekday() == 6:
                    settle_date += timedelta(days=1)

                port = np.random.choice(portfolios)
                cpty = np.random.choice(counterparties)
                inst = np.random.choice(instruments)
                ttype = np.random.choice(txn_types)
                curr = np.random.choice(currencies)

                qty = float(np.random.randint(10, 200) * 50)
                price = round(float(np.random.uniform(95.0, 105.0)), 4)
                settled_amount = round(qty * price, 2)

                records.append({
                    "TRANSACTION_ID": txn_id,
                    "TRANSACTION_DATE": current_date,
                    "SETTLEMENT_DATE": settle_date,
                    "PORTFOLIO_ID": port,
                    "COUNTERPARTY_ID": cpty,
                    "INSTRUMENT_ID": inst,
                    "TRANSACTION_TYPE": ttype,
                    "QUANTITY": qty,
                    "PRICE": price,
                    "SETTLED_AMOUNT": settled_amount,
                    "CURRENCY": curr
                })

            # ---------------------------------------------------------------
            # INJECTED GROUND-TRUTH ANOMALY 1: Fat-Finger Trade
            # Date: 2026-04-14 | Massive settled amount ($45,000,000)
            # ---------------------------------------------------------------
            if current_date == date(2026, 4, 14):
                records.append({
                    "TRANSACTION_ID": "TXN_20260414_FAT_FINGER_ANOMALY",
                    "TRANSACTION_DATE": date(2026, 4, 14),
                    "SETTLEMENT_DATE": date(2026, 4, 16),
                    "PORTFOLIO_ID": "PORT_001",
                    "COUNTERPARTY_ID": "CPTY_011",
                    "INSTRUMENT_ID": "INST_101",
                    "TRANSACTION_TYPE": "BUY",
                    "QUANTITY": 450000.0,
                    "PRICE": 100.00,
                    "SETTLED_AMOUNT": 45000000.00,
                    "CURRENCY": "USD"
                })

            # ---------------------------------------------------------------
            # INJECTED GROUND-TRUTH ANOMALY 2: Flash-Pricing Error
            # Date: 2026-05-22 | Severe pricing error ($12.50 vs $100 normal)
            # ---------------------------------------------------------------
            if current_date == date(2026, 5, 22):
                records.append({
                    "TRANSACTION_ID": "TXN_20260522_FLASH_PRICE_ANOMALY",
                    "TRANSACTION_DATE": date(2026, 5, 22),
                    "SETTLEMENT_DATE": date(2026, 5, 26),
                    "PORTFOLIO_ID": "PORT_002",
                    "COUNTERPARTY_ID": "CPTY_012",
                    "INSTRUMENT_ID": "INST_104",
                    "TRANSACTION_TYPE": "BUY",
                    "QUANTITY": 20000.0,
                    "PRICE": 12.50,
                    "SETTLED_AMOUNT": 250000.00,
                    "CURRENCY": "USD"
                })

            # ---------------------------------------------------------------
            # INJECTED GROUND-TRUTH ANOMALY 4: Runaway Algo Burst
            # Date: 2026-07-29 | 95 rapid-fire trades from PORT_003
            # ---------------------------------------------------------------
            if current_date == date(2026, 7, 29):
                algo_insts = ["INST_129", "INST_130", "INST_131", "INST_132", "INST_142"]
                for burst_idx in range(1, 96):
                    b_qty = float(np.random.randint(20, 50) * 100)
                    b_price = round(float(np.random.uniform(98.0, 102.0)), 4)
                    b_settled_amt = round(b_qty * b_price, 2)
                    records.append({
                        "TRANSACTION_ID": f"TXN_20260729_ALGO_BURST_{burst_idx:03d}",
                        "TRANSACTION_DATE": date(2026, 7, 29),
                        "SETTLEMENT_DATE": date(2026, 7, 31),
                        "PORTFOLIO_ID": "PORT_003",
                        "COUNTERPARTY_ID": "CPTY_043",  # Jane Street Capital LLC
                        "INSTRUMENT_ID": algo_insts[burst_idx % len(algo_insts)],
                        "TRANSACTION_TYPE": "BUY" if burst_idx % 2 == 0 else "SELL",
                        "QUANTITY": b_qty,
                        "PRICE": b_price,
                        "SETTLED_AMOUNT": b_settled_amt,
                        "CURRENCY": "USD"
                    })

        current_date += timedelta(days=1)

    df = pd.DataFrame(records)
    return df


def generate_fact_transactions(yyyymm: str = None, yyyymmdd: str = None, base_dir: str = None):
    """Generates transaction Parquet datasets in daily or monthly partition folders."""
    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    output_base = os.path.join(base_dir, "outputFile", "TRANSACTIONS")

    if yyyymmdd:
        # Daily Mode
        target_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        month_str = target_date.strftime("%Y%m")
        output_dir = os.path.join(output_base, f"DATA_{month_str}")
        os.makedirs(output_dir, exist_ok=True)

        file_name = f"TRANSACTIONS_DAILY_{yyyymmdd}.parquet"
        file_path = os.path.join(output_dir, file_name)

        df = generate_transactions_for_range(target_date, target_date)
        df.to_parquet(file_path, index=False)
        print(f"[TRANSACTIONS] Daily trades generated: {file_path} ({len(df)} rows)")
        return file_path

    else:
        # Monthly Batch Mode
        if not yyyymm:
            yyyymm = get_default_last_month()

        year = int(yyyymm[:4])
        month = int(yyyymm[4:6])

        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year, 12, 31)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)

        output_dir = os.path.join(output_base, f"DATA_{yyyymm}")
        os.makedirs(output_dir, exist_ok=True)

        file_name = f"TRANSACTIONS_MONTHLY_{yyyymm}.parquet"
        file_path = os.path.join(output_dir, file_name)

        df = generate_transactions_for_range(start_date, end_date)
        df.to_parquet(file_path, index=False)
        print(f"[TRANSACTIONS] Monthly trades generated: {file_path} ({len(df)} rows)")
        return file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ABOR Transactions Parquet Dataset")
    parser.add_argument("--yyyymm", "-m", type=str, help="Target month in YYYYMM format (e.g. 202603)")
    parser.add_argument("--yyyymmdd", "-d", type=str, help="Target date in YYYYMMDD format (e.g. 20260315)")
    parser.add_argument("--all-months", action="store_true", help="Generate monthly batches for 202601 to 202609")
    args = parser.parse_args()

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    if args.all_months:
        print("[START] Generating FACT_SETTLED_TRANSACTIONS datasets for all 9 months (202601 to 202609)...\n")
        total_rows = 0
        for m in [f"20260{i}" for i in range(1, 10)]:
            fp = generate_fact_transactions(yyyymm=m, base_dir=project_root)
            df_check = pd.read_parquet(fp)
            total_rows += len(df_check)
        print(f"\n[COMPLETE] Batch transactions generation finished! Cumulative rows across 9 months: {total_rows}")
    elif args.yyyymmdd:
        generate_fact_transactions(yyyymmdd=args.yyyymmdd, base_dir=project_root)
    else:
        month_to_run = args.yyyymm or get_default_last_month()
        generate_fact_transactions(yyyymm=month_to_run, base_dir=project_root)
