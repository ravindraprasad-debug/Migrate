"""
Module: primary_key_check.py
Description: Evaluates primary key space alignment between Source (Azure) and Target (AWS) DataFrames.
Takes set differences in both directions:
1. Source Keys MINUS Target Keys -> Identifies missing IDs in AWS.
2. Target Keys MINUS Source Keys -> Identifies extra IDs in AWS.
"""

import json
from typing import Dict, Any, List
from pyspark.sql import DataFrame


def run_primary_key_check(
    source_df: DataFrame,
    target_df: DataFrame,
    primary_keys: List[str]
) -> Dict[str, Any]:
    """
    Performs set difference analysis across business primary key columns.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        primary_keys (List[str]): Primary key column names.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and missing/extra key counts.
    """
    valid_pks = [pk for pk in primary_keys if pk in source_df.columns and pk in target_df.columns]

    if not valid_pks:
        return {
            "check_name": "primary_key_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "No valid primary keys specified or found in DataFrames"})
        }

    # Extract distinct key sets
    src_keys = source_df.select(valid_pks).distinct()
    tgt_keys = target_df.select(valid_pks).distinct()

    # Calculate set differences in both directions
    missing_in_target_df = src_keys.subtract(tgt_keys)
    extra_in_target_df = tgt_keys.subtract(src_keys)

    missing_count = missing_in_target_df.count()
    extra_count = extra_in_target_df.count()

    total_discrepancies = missing_count + extra_count
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    # Sample top missing and extra key values for audit report
    missing_sample = [r.asDict() for r in missing_in_target_df.limit(20).collect()]
    extra_sample = [r.asDict() for r in extra_in_target_df.limit(20).collect()]

    details = {
        "primary_keys": valid_pks,
        "missing_in_aws_count": missing_count,
        "extra_in_aws_count": extra_count,
        "missing_in_aws_sample": missing_sample,
        "extra_in_aws_sample": extra_sample
    }

    return {
        "check_name": "primary_key_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(src_keys.count(), tgt_keys.count()),
        "details_json": json.dumps(details)
    }
