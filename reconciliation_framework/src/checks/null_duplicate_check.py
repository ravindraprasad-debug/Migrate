"""
Module: null_duplicate_check.py
Description: Validates column-level null counts and detects duplicate records on business primary keys.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when


def run_null_duplicate_check(
    source_df: DataFrame,
    target_df: DataFrame,
    critical_columns: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Compares column null counts for critical fields and checks for duplicate primary keys.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        critical_columns (Optional[List[str]]): List of columns to check for null count parity.
        primary_keys (Optional[List[str]]): Business primary key columns to check for duplicate records.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and breakdown details.
    """
    if critical_columns is None:
        critical_columns = source_df.columns

    valid_critical = [c for c in critical_columns if c in source_df.columns and c in target_df.columns]

    # Calculate null counts for critical columns on source and target
    src_null_exprs = [count(when(col(c).isNull(), 1)).alias(f"src_null_{c}") for c in valid_critical]
    tgt_null_exprs = [count(when(col(c).isNull(), 1)).alias(f"tgt_null_{c}") for c in valid_critical]

    src_nulls = source_df.select(*src_null_exprs).collect()[0].asDict()
    tgt_nulls = target_df.select(*tgt_null_exprs).collect()[0].asDict()

    null_mismatches = []
    for c in valid_critical:
        s_n = src_nulls.get(f"src_null_{c}", 0)
        t_n = tgt_nulls.get(f"tgt_null_{c}", 0)
        if s_n != t_n:
            null_mismatches.append({
                "column": c,
                "source_null_count": s_n,
                "target_null_count": t_n,
                "difference": abs(s_n - t_n)
            })

    # Detect duplicate primary keys
    src_dup_count = 0
    tgt_dup_count = 0
    valid_pks = [pk for pk in (primary_keys or []) if pk in source_df.columns and pk in target_df.columns]

    if valid_pks:
        src_dup_df = source_df.groupBy(valid_pks).agg(count("*").alias("pk_count")).filter("pk_count > 1")
        tgt_dup_df = target_df.groupBy(valid_pks).agg(count("*").alias("pk_count")).filter("pk_count > 1")

        src_dup_count = src_dup_df.count()
        tgt_dup_count = tgt_dup_df.count()

    total_discrepancies = len(null_mismatches) + abs(src_dup_count - tgt_dup_count) + src_dup_count + tgt_dup_count
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "critical_columns_checked": valid_critical,
        "null_mismatches": null_mismatches,
        "source_duplicate_pk_rows": src_dup_count,
        "target_duplicate_pk_rows": tgt_dup_count,
        "primary_keys_evaluated": valid_pks
    }

    return {
        "check_name": "null_duplicate_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(source_df.count(), target_df.count()),
        "details_json": json.dumps(details)
    }
