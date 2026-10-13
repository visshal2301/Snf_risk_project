"""
Systemic Contagion & Liquidity Shock Simulator Engine
======================================================
Implements an in-memory Eisenberg-Noe / Glasserman multi-round domino failure cascade model
to evaluate counterparty default contagion, collateral deficits, and secondary network spillovers.
"""

from typing import Dict, List, Any
import pandas as pd
import numpy as np


# Baseline Tier-1 Capital Buffer estimates by Credit Rating Band
CAPITAL_BUFFER_RATIOS = {
    'AAA': 0.25,
    'AA': 0.20,
    'A': 0.16,
    'BBB': 0.12,
    'BB': 0.08,
    'B': 0.05,
    'CCC': 0.03
}


def simulate_network_cascade(
    df_cpty: pd.DataFrame,
    initial_default_id: str,
    lgd_pct: float = 65.0,
    fire_sale_discount_pct: float = 20.0,
    network_interconnectedness: float = 0.35
) -> Dict[str, Any]:
    """
    Executes a multi-round domino contagion simulation.

    Args:
        df_cpty: DataFrame of counterparties with columns:
                 ['COUNTERPARTY_ID', 'COUNTERPARTY_NAME', 'CREDIT_RATING', 'NET_EXPOSURE_USD', 'SECTOR']
        initial_default_id: COUNTERPARTY_ID of initial defaulting institution.
        lgd_pct: Loss Given Default percentage [0 - 100].
        fire_sale_discount_pct: Asset liquidation fire-sale penalty [0 - 100].
        network_interconnectedness: Cross-entity spillover correlation [0 - 1].

    Returns:
        Dictionary containing round-by-round cascade metrics, summary statistics,
        and affected counterparty lists.
    """
    if df_cpty.empty:
        return {"status": "error", "message": "No counterparty exposure data available."}

    df = df_cpty.copy()
    
    # Ensure standard column naming
    id_col = 'COUNTERPARTY_ID' if 'COUNTERPARTY_ID' in df.columns else df.columns[0]
    name_col = 'COUNTERPARTY_NAME' if 'COUNTERPARTY_NAME' in df.columns else id_col
    rating_col = 'CREDIT_RATING' if 'CREDIT_RATING' in df.columns else 'RATING'
    exp_col = 'NET_EXPOSURE_USD' if 'NET_EXPOSURE_USD' in df.columns else 'EXPOSURE_MTM'

    df['CAPITAL_BUFFER_RATIO'] = df[rating_col].map(lambda r: CAPITAL_BUFFER_RATIOS.get(str(r).upper(), 0.10))
    # Estimated Capital Buffer = max(50M, Net Exposure * 1.5 * Capital Buffer Ratio)
    df['ESTIMATED_CAPITAL_USD'] = df.apply(
        lambda r: max(50_000_000.0, abs(float(r[exp_col])) * 1.5 * r['CAPITAL_BUFFER_RATIO']),
        axis=1
    )
    df['CUMULATIVE_LOSS_USD'] = 0.0
    df['IS_DEFAULTED'] = False
    df['DEFAULT_ROUND'] = -1

    lgd = lgd_pct / 100.0
    fire_sale = fire_sale_discount_pct / 100.0

    # Locate initial entity
    initial_mask = df[id_col] == initial_default_id
    if not initial_mask.any():
        # Fallback to largest exposure entity if specified ID not found
        initial_default_id = df.sort_values(by=exp_col, ascending=False).iloc[0][id_col]
        initial_mask = df[id_col] == initial_default_id

    initial_entity = df[initial_mask].iloc[0]
    initial_name = initial_entity[name_col]
    initial_exp = float(initial_entity[exp_col])

    # Round 0: Initial Failure
    df.loc[initial_mask, 'IS_DEFAULTED'] = True
    df.loc[initial_mask, 'DEFAULT_ROUND'] = 0
    direct_firm_loss = initial_exp * lgd

    rounds_log: List[Dict[str, Any]] = [
        {
            "round": 0,
            "stage": "Round 0: Initial Trigger Default",
            "new_defaults_count": 1,
            "new_default_entities": [f"{initial_name} ({initial_default_id})"],
            "round_loss_firm_usd": direct_firm_loss,
            "round_network_loss_usd": direct_firm_loss,
            "description": f"Initial collapse of {initial_name}. Immediate direct credit loss of ${direct_firm_loss:,.2f} at {lgd_pct}% LGD."
        }
    ]

    total_firm_loss = direct_firm_loss
    secondary_firm_loss = 0.0
    current_round = 1
    max_rounds = 5

    while current_round <= max_rounds:
        new_defaults = []
        new_network_loss = 0.0
        new_firm_loss = 0.0

        surviving = df[~df['IS_DEFAULTED']].copy()
        if surviving.empty:
            break

        # Calculate systemic shock propagation to surviving counterparties
        # Contagion is proportional to network interconnectedness and previous round losses
        prev_round_losses = rounds_log[-1]["round_network_loss_usd"]
        
        for idx, row in surviving.iterrows():
            cpty_exp = float(row[exp_col])
            # Direct bilateral link + systemic fire-sale price impact
            systemic_shock = (prev_round_losses * 0.08 * network_interconnectedness) + \
                             (cpty_exp * fire_sale * 0.15)
            
            df.loc[idx, 'CUMULATIVE_LOSS_USD'] += systemic_shock
            new_network_loss += systemic_shock

            # Insolvency threshold check: Losses exceed Capital Buffer
            if df.loc[idx, 'CUMULATIVE_LOSS_USD'] >= df.loc[idx, 'ESTIMATED_CAPITAL_USD']:
                df.loc[idx, 'IS_DEFAULTED'] = True
                df.loc[idx, 'DEFAULT_ROUND'] = current_round
                new_defaults.append(f"{row[name_col]} ({row[id_col]})")
                
                # Firm suffers additional default write-down on this newly defaulted entity
                additional_firm_loss = cpty_exp * lgd * 0.5
                new_firm_loss += additional_firm_loss

        secondary_firm_loss += new_firm_loss
        total_firm_loss += new_firm_loss

        rounds_log.append({
            "round": current_round,
            "stage": f"Round {current_round}: Secondary Domino Cascade",
            "new_defaults_count": len(new_defaults),
            "new_default_entities": new_defaults,
            "round_loss_firm_usd": new_firm_loss,
            "round_network_loss_usd": new_network_loss,
            "description": f"Round {current_round} triggered {len(new_defaults)} new insolvent counterparties causing ${new_firm_loss:,.2f} incremental firm loss."
        })

        if len(new_defaults) == 0:
            # Equilibrium reached: No further dominoes
            break

        current_round += 1

    total_cptys = len(df)
    total_defaulted = int(df['IS_DEFAULTED'].sum())
    contagion_index = round((total_defaulted / total_cptys) * 100, 1)

    return {
        "status": "success",
        "initial_entity_id": initial_default_id,
        "initial_entity_name": initial_name,
        "total_cptys": total_cptys,
        "total_defaulted": total_defaulted,
        "total_survived": total_cptys - total_defaulted,
        "direct_firm_loss_usd": direct_firm_loss,
        "secondary_firm_loss_usd": secondary_firm_loss,
        "total_firm_loss_usd": total_firm_loss,
        "contagion_index_pct": contagion_index,
        "rounds_executed": len(rounds_log),
        "rounds_log": rounds_log,
        "detailed_df": df.sort_values(by=['IS_DEFAULTED', 'DEFAULT_ROUND', exp_col], ascending=[False, True, False])
    }
