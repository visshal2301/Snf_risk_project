import json
from pathlib import Path
import pytest

def test_metadata_contracts_exist_and_valid():
    """Verify that all ABOR schema contracts exist and are valid JSON."""
    contracts_dir = Path("abor_risk/metadata")
    assert contracts_dir.exists(), "Metadata directory must exist"
    
    expected_contracts = [
        "COUNTERPARTY_schema.json",
        "INSTRUMENT_schema.json",
        "PORTFOLIO_schema.json",
        "POSITIONS_schema.json",
        "TRANSACTIONS_schema.json"
    ]
    
    for contract_name in expected_contracts:
        contract_path = contracts_dir / contract_name
        assert contract_path.exists(), f"Contract {contract_name} is missing"
        
        with open(contract_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert isinstance(data, dict), f"{contract_name} must be a valid JSON object"
            assert "columns" in data or "fields" in data or len(data) > 0, f"{contract_name} has invalid structure"
