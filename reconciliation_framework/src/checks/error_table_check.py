"""
Module: error_table_check.py
Description: Compares dead-letter / rejected record tables between Source (Azure) and Target (AWS).
Verifies that both environments reject the same rows for the same validation reasons.
"""

import json
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col


def run_error_table_check(
    error_source_df: Optional[DataFrame],
    error_target_df: Optional[DataFrame]
) -> Dict[str, Any]:
    """
    Compares rejected record counts and rejection reason distributions.
    
    Args:
        error_source_df (Optional[DataFrame]): Source error/dead-letter PySpark DataFrame.
        error_target_df (Optional[DataFrame]): Target error/dead-letter PySpark DataFrame.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and dead-letter metrics.
    """
    if error_source_df is None or error_target_df is None:
        return {
            "check_name": "error_table_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "Error / dead-letter tables not provided or not configured"})
        }

    src_count = error_source_df.count()
    tgt_count = error_target_df.count()

    count_diff = abs(src_count - tgt_count)

    # Discover rejection reason column name
    reason_col = None
    for candidate in ["reject_reason", "error_message", "error_reason", "rejection_code"]:
        if candidate in error_source_df.columns and candidate in error_target_df.columns:
            reason_col = candidate
            break

    reason_mismatches = []
    # If rejection reason column exists, group by reason and compare distribution
    if reason_col:
        src_reasons = error_source_df.groupBy(reason_col).agg(count("*").alias("src_reject_count"))
        tgt_reasons = error_target_df.groupBy(reason_col).agg(count("*").alias("tgt_reject_count"))

        joined = src_reasons.join(tgt_reasons, on=reason_col, how="full_outer")
        diff_df = joined.filter(
            (col("src_reject_count").isNull()) |
            (col("tgt_reject_count").isNull()) |
            (col("src_reject_count") != col("tgt_reject_count"))
        )

        for row in diff_df.collect():
            reason_val = str(row[reason_col])
            s_c = row["src_reject_count"] if row["src_reject_count"] is not None else 0
            t_c = row["tgt_reject_count"] if row["tgt_reject_count"] is not None else 0
            reason_mismatches.append({
                "reason": reason_val,
                "source_rejected_count": s_c,
                "target_rejected_count": t_c,
                "difference": abs(s_c - t_c)
            })

    total_discrepancies = count_diff + len(reason_mismatches)
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "source_rejected_total": src_count,
        "target_rejected_total": tgt_count,
        "rejected_count_difference": count_diff,
        "evaluated_reason_column": reason_col,
        "rejection_reason_mismatches": reason_mismatches
    }

    return {
        "check_name": "error_table_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(src_count, tgt_count),
        "details_json": json.dumps(details)
    }
