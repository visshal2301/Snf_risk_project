"""
Schema Contract Validator Module
=================================
Validates incoming Parquet files against subject area JSON metadata contracts.
Logs verification details for audit and debugging purposes.

Rules Enforced:
1. Missing Required Column -> ABORT execution with detailed log.
2. Data Type Mismatch       -> ABORT execution with detailed log.
3. Extra / Surplus Column   -> LOG INFO message and PROCEED (allowed).
"""

import os
import json
import sys
import pyarrow.parquet as pq
import pyarrow as pa


TYPE_COMPATIBILITY_MAP = {
    "string": ["string", "utf8", "large_string"],
    "date": ["date32[day]", "date64", "timestamp[us]", "timestamp[ms]", "timestamp[ns]", "date32"],
    "bool": ["bool", "boolean"],
    "integer": ["int64", "int32", "int16", "int8"],
    "number": ["double", "float", "int64", "int32", "decimal128"]
}


def validate_parquet_schema(parquet_file_path: str, subject_area: str, metadata_dir: str, logger=None) -> bool:
    """
    Validates a Parquet file against the metadata schema contract for the given subject area.

    Returns True if valid. Aborts execution (sys.exit(1)) if missing columns or incompatible types.
    """
    def _log_info(msg):
        if logger:
            logger.info(msg)
        else:
            print(f"[INFO] {msg}")

    def _log_error(msg):
        if logger:
            logger.error(msg)
        else:
            print(f"[ERROR] {msg}")

    schema_file_name = f"{subject_area}_schema.json"
    schema_file_path = os.path.join(metadata_dir, schema_file_name)

    _log_info(f"Inspecting Data Contract Metadata: {schema_file_path}")

    if not os.path.exists(schema_file_path):
        _log_error(f"Schema contract file not found: {schema_file_path}")
        sys.exit(1)

    # 1. Load Expected Metadata Schema
    with open(schema_file_path, "r", encoding="utf-8") as f:
        metadata_contract = json.load(f)

    expected_columns = {col["name"]: col for col in metadata_contract.get("columns", [])}

    # 2. Inspect Parquet File Schema using PyArrow
    try:
        parquet_file = pq.ParquetFile(parquet_file_path)
        parquet_schema = parquet_file.schema.to_arrow_schema()
    except Exception as e:
        _log_error(f"Failed to read Parquet schema from file '{parquet_file_path}': {e}")
        sys.exit(1)

    actual_columns = {field.name: str(field.type) for field in parquet_schema}

    # 3. Check for Missing Columns (CRITICAL - ABORT IF MISSING)
    missing_columns = [col_name for col_name in expected_columns if col_name not in actual_columns]

    if missing_columns:
        _log_error(f"SCHEMA VALIDATION ABORTED | Subject Area: '{subject_area}'")
        _log_error(f"File Path: {parquet_file_path}")
        _log_error(f"Missing Required Column(s): {missing_columns}")
        _log_error("Action: Aborting ingestion to prevent database corruption.")
        sys.exit(1)

    # 4. Check for Incompatible Data Types (CRITICAL - ABORT IF INCOMPATIBLE)
    type_mismatches = []
    for col_name, col_meta in expected_columns.items():
        actual_type = actual_columns[col_name]
        expected_logical_type = col_meta["type"].lower()
        allowed_types = TYPE_COMPATIBILITY_MAP.get(expected_logical_type, [expected_logical_type])

        is_compatible = any(actual_type.startswith(allowed) or allowed in actual_type for allowed in allowed_types)
        if not is_compatible:
            type_mismatches.append(f"Column '{col_name}': Expected '{expected_logical_type}', found '{actual_type}'")

    if type_mismatches:
        _log_error(f"SCHEMA VALIDATION ABORTED | Subject Area: '{subject_area}'")
        _log_error(f"File Path: {parquet_file_path}")
        _log_error(f"Data Type Mismatch(es): {type_mismatches}")
        _log_error("Action: Aborting ingestion due to schema drift.")
        sys.exit(1)

    # 5. Check for Extra / Surplus Columns (ALLOWED - LOG INFO)
    extra_columns = [col for col in actual_columns if col not in expected_columns]
    if extra_columns:
        _log_info(f"Extra column(s) detected in '{os.path.basename(parquet_file_path)}': {extra_columns} (Allowed per Additive Policy)")

    _log_info(f"Parquet schema contract validation PASSED for '{os.path.basename(parquet_file_path)}' against '{schema_file_name}'.")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python schema_validator.py <parquet_file_path> <subject_area>")
        sys.exit(1)

    file_path = sys.argv[1]
    subject = sys.argv[2]
    meta_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "metadata"))
    validate_parquet_schema(file_path, subject, meta_dir)
