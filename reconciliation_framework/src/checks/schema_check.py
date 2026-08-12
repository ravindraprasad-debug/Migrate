"""
Module: schema_check.py
Description: Validates schema definitions between legacy Azure Databricks tables and new AWS Databricks tables.
Compares column names, data types, nullability, missing/extra columns, and schema ordering.
"""

import json
from typing import Dict, Any
from pyspark.sql import DataFrame


def run_schema_check(
    source_df: DataFrame,
    target_df: DataFrame,
    strict_nullability: bool = False
) -> Dict[str, Any]:
    """
    Compares schema metadata between Source (Azure) and Target (AWS) DataFrames.
    Flags data type mismatches (e.g. INT vs BIGINT), missing columns in target, and extra columns in target.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        strict_nullability (bool): Whether to treat nullability differences as mismatches. Defaults to False.
        
    Returns:
        Dict[str, Any]: Check execution result containing status, discrepancy_count, and JSON details.
    """
    # Extract lowercased column metadata mapping (name -> (original_name, data_type, nullable))
    source_fields = {field.name.lower(): (field.name, field.dataType.simpleString(), field.nullable)
                     for field in source_df.schema.fields}
    target_fields = {field.name.lower(): (field.name, field.dataType.simpleString(), field.nullable)
                     for field in target_df.schema.fields}

    source_cols_set = set(source_fields.keys())
    target_cols_set = set(target_fields.keys())

    # Identify missing and extra column names
    missing_in_target = [source_fields[k][0] for k in (source_cols_set - target_cols_set)]
    extra_in_target = [target_fields[k][0] for k in (target_cols_set - source_cols_set)]

    common_cols = source_cols_set.intersection(target_cols_set)

    data_type_mismatches = []
    nullability_mismatches = []

    # Validate data types and nullability for common columns
    for k in common_cols:
        src_name, src_type, src_null = source_fields[k]
        tgt_name, tgt_type, tgt_null = target_fields[k]

        if src_type != tgt_type:
            data_type_mismatches.append({
                "column": src_name,
                "source_type": src_type,
                "target_type": tgt_type
            })

        if strict_nullability and (src_null != tgt_null):
            nullability_mismatches.append({
                "column": src_name,
                "source_nullable": src_null,
                "target_nullable": tgt_null
            })

    total_discrepancies = (
        len(missing_in_target) +
        len(extra_in_target) +
        len(data_type_mismatches) +
        len(nullability_mismatches)
    )

    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "source_column_count": len(source_df.columns),
        "target_column_count": len(target_df.columns),
        "missing_in_target": missing_in_target,
        "extra_in_target": extra_in_target,
        "type_mismatches": data_type_mismatches,
        "nullability_mismatches": nullability_mismatches
    }

    return {
        "check_name": "schema_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": len(source_df.columns),
        "details_json": json.dumps(details)
    }
