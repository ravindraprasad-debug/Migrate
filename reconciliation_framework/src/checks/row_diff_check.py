"""
Module: row_diff_check.py
Description: Performs full row-level differencing using EXCEPT / anti-joins:
1. Source EXCEPT Target -> Identifies rows missing in AWS.
2. Target EXCEPT Source -> Identifies extra rows in AWS.
Isolates column-level attribute differences for matching primary keys.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def run_row_diff_check(
    source_df: DataFrame,
    target_df: DataFrame,
    primary_keys: Optional[List[str]] = None,
    compare_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Isolates specific differing rows and attribute column values between Source and Target DataFrames.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        primary_keys (Optional[List[str]]): List of primary key columns.
        compare_columns (Optional[List[str]]): Subset of columns to evaluate.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and row-level diff samples.
    """
    common_cols = [c for c in (compare_columns or source_df.columns)
                   if c in source_df.columns and c in target_df.columns]

    if not common_cols:
        return {
            "check_name": "row_diff_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "No common columns to compare"})
        }

    src_sub = source_df.select(common_cols)
    tgt_sub = target_df.select(common_cols)

    # Set differencing across all common columns
    missing_in_target_df = src_sub.exceptAll(tgt_sub) if hasattr(src_sub, "exceptAll") else src_sub.subtract(tgt_sub)
    extra_in_target_df = tgt_sub.exceptAll(src_sub) if hasattr(tgt_sub, "exceptAll") else tgt_sub.subtract(src_sub)

    missing_count = missing_in_target_df.count()
    extra_count = extra_in_target_df.count()

    column_mismatches_sample = []
    valid_pks = [pk for pk in (primary_keys or []) if pk in common_cols]

    # If key alignment is available and discrepancies exist, pinpoint exact column mismatches
    if valid_pks and (missing_count > 0 or extra_count > 0):
        joined = src_sub.alias("src").join(
            tgt_sub.alias("tgt"),
            on=valid_pks,
            how="inner"
        )
        non_pk_cols = [c for c in common_cols if c not in valid_pks]

        mismatch_conditions = []
        for c in non_pk_cols:
            mismatch_conditions.append(
                (col(f"src.{c}").isNotNull() & col(f"tgt.{c}").isNull()) |
                (col(f"src.{c}").isNull() & col(f"tgt.{c}").isNotNull()) |
                (col(f"src.{c}") != col(f"tgt.{c}"))
            )

        if mismatch_conditions:
            combined_cond = mismatch_conditions[0]
            for cond in mismatch_conditions[1:]:
                combined_cond = combined_cond | cond

            attr_diff_df = joined.filter(combined_cond)
            for row in attr_diff_df.limit(20).collect():
                pk_vals = {pk: row[pk] for pk in valid_pks}
                col_diffs = {}
                for c in non_pk_cols:
                    s_v = row[f"src.{c}"]
                    t_v = row[f"tgt.{c}"]
                    if s_v != t_v:
                        col_diffs[c] = {"source": str(s_v), "target": str(t_v)}
                column_mismatches_sample.append({
                    "primary_key": pk_vals,
                    "differing_columns": col_diffs
                })

    total_discrepancies = missing_count + extra_count
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "missing_in_target_count": missing_count,
        "extra_in_target_count": extra_count,
        "sample_missing_rows": [r.asDict() for r in missing_in_target_df.limit(10).collect()],
        "sample_extra_rows": [r.asDict() for r in extra_in_target_df.limit(10).collect()],
        "column_attribute_mismatches_sample": column_mismatches_sample
    }

    return {
        "check_name": "row_diff_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(source_df.count(), target_df.count()),
        "details_json": json.dumps(details)
    }
