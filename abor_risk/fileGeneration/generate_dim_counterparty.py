"""
DIM_COUNTERPARTY Parquet Data Generator (50 Entities Universe)
==============================================================
Generates synthetic institutional counterparty reference data with dynamic
SCD Type 2 rating migrations for months 202601 through 202609 (9 months).

Covers 50 counterparties spanning 6 key institutional sectors:
  - Commercial Banks
  - Prime Brokers
  - Hedge Funds
  - Asset Managers
  - Pension Funds
  - Sovereign Wealth & Insurance

Output Path Structure:
  .../abor_risk/outputFile/COUNTERPARTY/DATA_YYYYMM/dim_counterparty_YYYYMM.parquet

Usage:
  python generate_dim_counterparty.py --all-months
  python generate_dim_counterparty.py --yyyymm 202605
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


def generate_dim_counterparty(yyyymm: str, base_dir: str):
    """
    Generates DIM_COUNTERPARTY dataset for the specified YYYYMM month,
    incorporating dynamic SCD Type 2 rating migrations up to that month.
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
    subject_area = "COUNTERPARTY"
    output_base = os.path.join(base_dir, "outputFile")
    if os.path.isfile(output_base):
        os.remove(output_base)

    output_dir = os.path.join(output_base, subject_area, f"DATA_{yyyymm}")
    os.makedirs(output_dir, exist_ok=True)

    output_file_name = f"dim_counterparty_{yyyymm}.parquet"
    output_file_path = os.path.join(output_dir, output_file_name)

    # Base Counterparty Entities Definitions (50 Institutional Entities)
    base_entities = {
        "CPTY_001": {"name": "Apex Global Pension Fund", "lei": "5493001KJ957B2X51Y12", "sector": "Pension Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_002": {"name": "Meridian Hedge Master LP", "lei": "213800X97G3A518B9984", "sector": "Hedge Fund", "jurisdiction": "UK", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_003": {"name": "Vanguard Sovereign Wealth", "lei": "54930099812A45BC7711", "sector": "Sovereign Wealth", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_004": {"name": "Titan Credit Opportunities", "lei": "54930033411B78CC8822", "sector": "Asset Manager", "jurisdiction": "US", "netting": "GMRA", "initial_rating": "BBB"},
        "CPTY_005": {"name": "Zenith Alpha Arbitrage Fund", "lei": "98450077812C99DD1133", "sector": "Hedge Fund", "jurisdiction": "KY", "netting": "NONE", "initial_rating": "BBB"},
        "CPTY_006": {"name": "Pacific Life & Annuity", "lei": "54930088123A11BB2244", "sector": "Insurance", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_007": {"name": "Barclays Capital Services", "lei": "213800K1234B5678C999", "sector": "Commercial Bank", "jurisdiction": "UK", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_008": {"name": "Nordic Pension Investment", "lei": "54930044112C33DD4455", "sector": "Pension Fund", "jurisdiction": "EU", "netting": "EMA", "initial_rating": "AAA"},
        "CPTY_009": {"name": "Highland Special Situations", "lei": "98450011223A44BB5566", "sector": "Asset Manager", "jurisdiction": "US", "netting": "GMRA", "initial_rating": "B"},
        "CPTY_010": {"name": "Tokyo Sovereign Reserve", "lei": "35380099112A33BB4455", "sector": "Sovereign Wealth", "jurisdiction": "JP", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_011": {"name": "JPMorgan Chase Global Markets", "lei": "7H6GLXDRUGQFU57RNE97", "sector": "Commercial Bank", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_012": {"name": "Goldman Sachs International", "lei": "W22LROWP2IHZNBB6K528", "sector": "Prime Broker", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_013": {"name": "Morgan Stanley Capital Services", "lei": "4PQUHN3IBFGFNF3FC683", "sector": "Prime Broker", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_014": {"name": "Citadel Securities Europe", "lei": "549300Q5V55V44P2W123", "sector": "Hedge Fund", "jurisdiction": "UK", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_015": {"name": "Millennium Capital Management", "lei": "549300R6W66W55Q3X456", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_016": {"name": "Point72 Asset Management", "lei": "549300S7X77X66R4Y789", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "NONE", "initial_rating": "BBB"},
        "CPTY_017": {"name": "Bridgewater Associates LP", "lei": "549300T8Y88Y77S5Z012", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_018": {"name": "Two Sigma Investments", "lei": "549300U9Z99Z88T6A345", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_019": {"name": "D.E. Shaw & Co Capital", "lei": "549300V0A00A99U7B678", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_020": {"name": "BlackRock Institutional Trust", "lei": "549300W1B11B00V8C901", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_021": {"name": "Fidelity Management Global", "lei": "549300X2C22C11W9D234", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_022": {"name": "PIMCO Global Advisors", "lei": "549300Y3D33D22X0E567", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_023": {"name": "Wellington Management Global", "lei": "549300Z4E44E33Y1F890", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_024": {"name": "State Street Global Advisors", "lei": "549300A5F55F44Z2G123", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_025": {"name": "CalPERS Investment Trust", "lei": "549300B6G66G55A3H456", "sector": "Pension Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_026": {"name": "Ontario Teachers Pension Plan", "lei": "549300C7H77H66B4I789", "sector": "Pension Fund", "jurisdiction": "CA", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_027": {"name": "Canada Pension Plan Investment", "lei": "549300D8I88I77C5J012", "sector": "Pension Fund", "jurisdiction": "CA", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_028": {"name": "New York State Common Fund", "lei": "549300E9J99J88D6K345", "sector": "Pension Fund", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_029": {"name": "Abu Dhabi Investment Authority", "lei": "549300F0K00K99E7L678", "sector": "Sovereign Wealth", "jurisdiction": "AE", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_030": {"name": "GIC Private Limited", "lei": "549300G1L11L00F8M901", "sector": "Sovereign Wealth", "jurisdiction": "SG", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_031": {"name": "Temasek Holdings", "lei": "549300H2M22M11G9N234", "sector": "Sovereign Wealth", "jurisdiction": "SG", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_032": {"name": "Norges Bank Investment", "lei": "549300I3N33N22H0O567", "sector": "Sovereign Wealth", "jurisdiction": "NO", "netting": "ISDA_CSA", "initial_rating": "AAA"},
        "CPTY_033": {"name": "BNP Paribas Prime Brokerage", "lei": "R0MUWS6PU8MPRO8K5P83", "sector": "Commercial Bank", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_034": {"name": "Deutsche Bank Global Markets", "lei": "7LTWFZYICNSX8D621K86", "sector": "Commercial Bank", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "BBB"},
        "CPTY_035": {"name": "UBS Investment Bank", "lei": "BFM8T61CT2L1QCEMIK50", "sector": "Commercial Bank", "jurisdiction": "CH", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_036": {"name": "Credit Agricole CIB", "lei": "1VUV7VQFKUOQSJ21A208", "sector": "Commercial Bank", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_037": {"name": "Prudential Financial Markets", "lei": "549300J4O44O33I1P890", "sector": "Insurance", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_038": {"name": "MetLife Institutional Capital", "lei": "549300K5P55P44J2Q123", "sector": "Insurance", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_039": {"name": "Allianz Global Investors", "lei": "549300L6Q66Q55K3R456", "sector": "Insurance", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_040": {"name": "AXA Investment Managers", "lei": "549300M7R77R66L4S789", "sector": "Insurance", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_041": {"name": "Munich Re Capital Markets", "lei": "549300N8S88S77M5T012", "sector": "Insurance", "jurisdiction": "EU", "netting": "ISDA_CSA", "initial_rating": "AA"},
        "CPTY_042": {"name": "Swiss Re Financial Products", "lei": "549300O9T99T88N6U345", "sector": "Insurance", "jurisdiction": "CH", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_043": {"name": "Jane Street Capital LLC", "lei": "549300P0U00U99O7V678", "sector": "Prime Broker", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_044": {"name": "Balyasny Asset Management", "lei": "549300Q1V11V00P8W901", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "GMRA", "initial_rating": "BBB"},
        "CPTY_045": {"name": "Marshall Wace Asset Management", "lei": "549300R2W22W11Q9X234", "sector": "Hedge Fund", "jurisdiction": "UK", "netting": "GMRA", "initial_rating": "BBB"},
        "CPTY_046": {"name": "AQR Capital Management", "lei": "549300S3X33X22R0Y567", "sector": "Asset Manager", "jurisdiction": "US", "netting": "ISDA_CSA", "initial_rating": "BBB"},
        "CPTY_047": {"name": "Man Group Investment Trust", "lei": "549300T4Y44Y33S1Z890", "sector": "Asset Manager", "jurisdiction": "UK", "netting": "ISDA_CSA", "initial_rating": "A"},
        "CPTY_048": {"name": "Brevan Howard Global Macro", "lei": "549300U5Z55Z44T2A123", "sector": "Hedge Fund", "jurisdiction": "UK", "netting": "NONE", "initial_rating": "BB"},
        "CPTY_049": {"name": "Hudson Bay Capital Master", "lei": "549300V6A66A55U3B456", "sector": "Hedge Fund", "jurisdiction": "US", "netting": "GMRA", "initial_rating": "BB"},
        "CPTY_050": {"name": "BlueMountain Credit Strategies", "lei": "549300W7B77B66V4C789", "sector": "Asset Manager", "jurisdiction": "US", "netting": "GMRA", "initial_rating": "B"}
    }

    # Chronological History of Rating Migration Events across 202601 - 202609
    migration_events = [
        # Event 1 (Feb 2026): CPTY_001 upgraded AA -> AAA
        {"cpty_id": "CPTY_001", "event_date": date(2026, 2, 15), "new_rating": "AAA"},
        # Event 2 (Mar 2026): CPTY_014 downgraded A -> BBB
        {"cpty_id": "CPTY_014", "event_date": date(2026, 3, 20), "new_rating": "BBB"},
        # Event 3 (Apr 2026): CPTY_004 upgraded BBB -> A
        {"cpty_id": "CPTY_004", "event_date": date(2026, 4, 1), "new_rating": "A"},
        # Event 4 (May 2026): CPTY_034 upgraded BBB -> A
        {"cpty_id": "CPTY_034", "event_date": date(2026, 5, 15), "new_rating": "A"},
        # Event 5 (Jun 2026): CPTY_002 downgraded AA -> A
        {"cpty_id": "CPTY_002", "event_date": date(2026, 6, 15), "new_rating": "A"},
        # Event 6 (Jul 2026): CPTY_048 downgraded BB -> B
        {"cpty_id": "CPTY_048", "event_date": date(2026, 7, 10), "new_rating": "B"},
        # Event 7 (Aug 2026): CPTY_005 downgraded BBB -> BB
        {"cpty_id": "CPTY_005", "event_date": date(2026, 8, 1), "new_rating": "BB"},
        # Event 8 (Aug 2026): CPTY_050 downgraded B -> CCC
        {"cpty_id": "CPTY_050", "event_date": date(2026, 8, 25), "new_rating": "CCC"},
        # Event 9 (Sep 2026): CPTY_009 downgraded B -> CCC
        {"cpty_id": "CPTY_009", "event_date": date(2026, 9, 1), "new_rating": "CCC"}
    ]

    records = []

    # Build SCD Type 2 history for each entity up to end_of_month_date
    for cpty_id, meta in base_entities.items():
        cpty_events = [
            e for e in migration_events
            if e["cpty_id"] == cpty_id and e["event_date"] <= end_of_month_date
        ]
        cpty_events = sorted(cpty_events, key=lambda x: x["event_date"])

        current_start_date = date(2025, 1, 1)
        current_rating = meta["initial_rating"]

        if not cpty_events:
            records.append({
                "COUNTERPARTY_ID": cpty_id,
                "COUNTERPARTY_NAME": meta["name"],
                "LEI_CODE": meta["lei"],
                "SECTOR": meta["sector"],
                "CREDIT_RATING": current_rating,
                "JURISDICTION": meta["jurisdiction"],
                "NETTING_AGREEMENT_TYPE": meta["netting"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })
        else:
            for ev in cpty_events:
                records.append({
                    "COUNTERPARTY_ID": cpty_id,
                    "COUNTERPARTY_NAME": meta["name"],
                    "LEI_CODE": meta["lei"],
                    "SECTOR": meta["sector"],
                    "CREDIT_RATING": current_rating,
                    "JURISDICTION": meta["jurisdiction"],
                    "NETTING_AGREEMENT_TYPE": meta["netting"],
                    "EFFECTIVE_START_DATE": current_start_date,
                    "EFFECTIVE_END_DATE": ev["event_date"] - timedelta(days=1),
                    "IS_ACTIVE": False
                })
                current_start_date = ev["event_date"]
                current_rating = ev["new_rating"]

            records.append({
                "COUNTERPARTY_ID": cpty_id,
                "COUNTERPARTY_NAME": meta["name"],
                "LEI_CODE": meta["lei"],
                "SECTOR": meta["sector"],
                "CREDIT_RATING": current_rating,
                "JURISDICTION": meta["jurisdiction"],
                "NETTING_AGREEMENT_TYPE": meta["netting"],
                "EFFECTIVE_START_DATE": current_start_date,
                "EFFECTIVE_END_DATE": date(9999, 12, 31),
                "IS_ACTIVE": True
            })

    df = pd.DataFrame(records)
    df = df.sort_values(by=["COUNTERPARTY_ID", "EFFECTIVE_START_DATE"]).reset_index(drop=True)

    df.to_parquet(output_file_path, engine="pyarrow", index=False)

    active_cnt = sum(df["IS_ACTIVE"])
    expired_cnt = sum(~df["IS_ACTIVE"])

    print(f"[SUCCESS] Generated DIM_COUNTERPARTY Parquet dataset for target month '{yyyymm}'.")
    print(f"   - File Path: {output_file_path}")
    print(f"   - Total Rows: {len(df)} ({active_cnt} Active, {expired_cnt} Historical SCD Type 2)")
    print(f"   - Format: Parquet (PyArrow Engine)")

    return output_file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DIM_COUNTERPARTY Parquet Files (202601 to 202609)")
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
        print(f"[START] Generating DIM_COUNTERPARTY datasets for all 9 months (202601 to 202609)...\n")
        for m in months_range:
            generate_dim_counterparty(m, base_directory)
        print("\n[COMPLETE] Batch generation finished for all 9 months!")
    else:
        target_month = args.yyyymm if args.yyyymm else get_default_last_month()
        generate_dim_counterparty(target_month, base_directory)
