"""
Module: incremental_check.py
Description: Validates incremental load, CDC, and MERGE/UPSERT operations by comparing date-bounded
or watermark incremental slices across insert, update, and delete streams.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, date_sub


def run_incremental_check(
    source_df: DataFrame,
    target_df: DataFrame,
    watermark_column: str,
    lookback_days: int = 1,
    primary_keys: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Filters source and target DataFrames by watermark lookback windows and checks incremental equivalence.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        watermark_column (str): Date/Timestamp column name used for incremental filtering.
        lookback_days (int): Lookback window in days. Defaults to 1.
        primary_keys (Optional[List[str]]): List of primary keys.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and incremental metrics.
    """
    if watermark_column not in source_df.columns or watermark_column not in target_df.columns:
        return {
            "check_name": "incremental_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({
                "reason": f"Watermark column '{watermark_column}' not found in DataFrames"
            })
        }

    # Filter incremental window
    inc_source = source_df.filter(
        col(watermark_column) >= date_sub(current_timestamp(), lookback_days)
    )
    inc_target = target_df.filter(
        col(watermark_column) >= date_sub(current_timestamp(), lookback_days)
    )

    src_inc_count = inc_source.count()
    tgt_inc_count = inc_target.count()

    count_diff = abs(src_inc_count - tgt_inc_count)

    missing_inc_count = 0
    extra_inc_count = 0
    valid_pks = [pk for pk in (primary_keys or []) if pk in source_df.columns and pk in target_df.columns]

    # Evaluate set differences across incremental primary key sets
    if valid_pks and (src_inc_count > 0 or tgt_inc_count > 0):
        src_keys = inc_source.select(valid_pks).distinct()
        tgt_keys = inc_target.select(valid_pks).distinct()

        missing_inc_count = src_keys.subtract(tgt_keys).count()
        extra_inc_count = tgt_keys.subtract(src_keys).count()

    total_discrepancies = count_diff + missing_inc_count + extra_inc_count
    status = "PASS" if total_discrepancies == 0 else "FAIL"

    details = {
        "watermark_column": watermark_column,
        "lookback_days": lookback_days,
        "incremental_source_row_count": src_inc_count,
        "incremental_target_row_count": tgt_inc_count,
        "count_difference": count_diff,
        "missing_incremental_keys_count": missing_inc_count,
        "extra_incremental_keys_count": extra_inc_count
    }

    return {
        "check_name": "incremental_check",
        "status": status,
        "discrepancy_count": total_discrepancies,
        "rows_compared": max(src_inc_count, tgt_inc_count),
        "details_json": json.dumps(details)
    }
