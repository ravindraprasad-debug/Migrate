"""
Module: aggregate_check.py
Description: Computes and compares metric aggregates (SUM, AVG, MIN, MAX, COUNT DISTINCT)
for numerical and business metric columns between Source (Azure) and Target (AWS) DataFrames.
Includes support for custom numerical tolerance thresholds.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum, avg, min as spark_min, max as spark_max, count, countDistinct, col


def run_aggregate_check(
    source_df: DataFrame,
    target_df: DataFrame,
    aggregate_columns: List[str],
    tolerance: float = 0.0
) -> Dict[str, Any]:
    """
    Computes statistical metric summaries across numeric columns and checks for parity within tolerance.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        aggregate_columns (List[str]): Numerical columns to evaluate.
        tolerance (float): Allowed floating point tolerance threshold for differences. Defaults to 0.0.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and metric comparison details.
    """
    valid_cols = [c for c in aggregate_columns if c in source_df.columns and c in target_df.columns]

    if not valid_cols:
        return {
            "check_name": "aggregate_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "No valid aggregate columns found in DataFrames"})
        }

    src_agg_exprs = []
    tgt_agg_exprs = []

    # Build aggregation expressions for each metric column
    for c in valid_cols:
        src_agg_exprs.extend([
            spark_sum(col(c)).alias(f"{c}_sum"),
            avg(col(c)).alias(f"{c}_avg"),
            spark_min(col(c)).alias(f"{c}_min"),
            spark_max(col(c)).alias(f"{c}_max"),
            countDistinct(col(c)).alias(f"{c}_distinct")
        ])
        tgt_agg_exprs.extend([
            spark_sum(col(c)).alias(f"{c}_sum"),
            avg(col(c)).alias(f"{c}_avg"),
            spark_min(col(c)).alias(f"{c}_min"),
            spark_max(col(c)).alias(f"{c}_max"),
            countDistinct(col(c)).alias(f"{c}_distinct")
        ])

    src_res = source_df.select(*src_agg_exprs).collect()[0].asDict()
    tgt_res = target_df.select(*tgt_agg_exprs).collect()[0].asDict()

    discrepancies = []

    # Compare source and target aggregates against specified tolerance
    for c in valid_cols:
        for metric in ["sum", "avg", "min", "max", "distinct"]:
            key = f"{c}_{metric}"
            src_val = src_res.get(key)
            tgt_val = tgt_res.get(key)

            if src_val is None and tgt_val is None:
                continue

            if src_val is None or tgt_val is None:
                discrepancies.append({
                    "column": c,
                    "metric": metric,
                    "source_value": src_val,
                    "target_value": tgt_val,
                    "difference": "NULL mismatch"
                })
                continue

            if isinstance(src_val, (int, float)) and isinstance(tgt_val, (int, float)):
                diff = abs(src_val - tgt_val)
                if diff > tolerance:
                    discrepancies.append({
                        "column": c,
                        "metric": metric,
                        "source_value": float(src_val),
                        "target_value": float(tgt_val),
                        "difference": float(diff),
                        "tolerance_allowed": tolerance
                    })
            elif src_val != tgt_val:
                discrepancies.append({
                    "column": c,
                    "metric": metric,
                    "source_value": str(src_val),
                    "target_value": str(tgt_val),
                    "difference": "Value mismatch"
                })

    status = "PASS" if len(discrepancies) == 0 else "FAIL"

    details = {
        "aggregate_columns_evaluated": valid_cols,
        "tolerance": tolerance,
        "discrepancies_count": len(discrepancies),
        "discrepancies_detail": discrepancies
    }

    return {
        "check_name": "aggregate_check",
        "status": status,
        "discrepancy_count": len(discrepancies),
        "rows_compared": len(valid_cols) * 5,
        "details_json": json.dumps(details)
    }
