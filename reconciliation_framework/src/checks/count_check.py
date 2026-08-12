"""
Module: count_check.py
Description: Performs total row count validation and partition-level row count validation between
Source (Azure) and Target (AWS) DataFrames.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col


def run_count_check(
    source_df: DataFrame,
    target_df: DataFrame,
    partition_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Validates overall table row counts and partition-by-partition row counts.
    Ensures that an overall count equality does not mask offset partition imbalances.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        partition_columns (Optional[List[str]]): List of partition columns for granular verification.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and breakdown metrics.
    """
    # Calculate total row counts
    source_count = source_df.count()
    target_count = target_df.count()

    count_diff = abs(source_count - target_count)
    partition_mismatches = []

    # Calculate partition-level counts if partition columns are supplied
    if partition_columns:
        valid_partitions = [p for p in partition_columns if p in source_df.columns and p in target_df.columns]
        if valid_partitions:
            src_part = source_df.groupBy(valid_partitions).agg(count("*").alias("src_count"))
            tgt_part = target_df.groupBy(valid_partitions).agg(count("*").alias("tgt_count"))

            # Outer join to find partition count discrepancies
            joined = src_part.join(tgt_part, on=valid_partitions, how="full_outer")
            diff_df = joined.filter(
                (col("src_count").isNull()) |
                (col("tgt_count").isNull()) |
                (col("src_count") != col("tgt_count"))
            )

            mismatch_rows = diff_df.collect()
            for r in mismatch_rows:
                part_val = {p: str(r[p]) for p in valid_partitions}
                src_c = r["src_count"] if r["src_count"] is not None else 0
                tgt_c = r["tgt_count"] if r["tgt_count"] is not None else 0
                partition_mismatches.append({
                    "partition": part_val,
                    "source_count": src_c,
                    "target_count": tgt_c,
                    "difference": abs(src_c - tgt_c)
                })

    total_discrepancies = count_diff + len(partition_mismatches)
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "source_total_count": source_count,
        "target_total_count": target_count,
        "overall_difference": count_diff,
        "partition_columns_evaluated": partition_columns or [],
        "partition_mismatches_count": len(partition_mismatches),
        "partition_mismatches_sample": partition_mismatches[:50]
    }

    return {
        "check_name": "count_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(source_count, target_count),
        "details_json": json.dumps(details)
    }
