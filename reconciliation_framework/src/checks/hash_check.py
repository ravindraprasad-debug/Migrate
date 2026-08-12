"""
Module: hash_check.py
Description: Computes deterministic SHA-256 hashes across normalized row values for both Source and Target
DataFrames to validate full row-level equivalence at scale.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils.string_normalizer import build_normalized_row_hash


def run_hash_check(
    source_df: DataFrame,
    target_df: DataFrame,
    columns_to_hash: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Applies string normalization and SHA-256 row hashing across Source and Target DataFrames.
    Compares aligned key hashes or set-difference hash distributions.
    
    Args:
        source_df (DataFrame): Source PySpark DataFrame.
        target_df (DataFrame): Target PySpark DataFrame.
        columns_to_hash (Optional[List[str]]): List of columns to include in SHA-256 calculation.
        primary_keys (Optional[List[str]]): Primary key columns to align row hashes.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and hash mismatch counts.
    """
    common_cols = [c for c in (columns_to_hash or source_df.columns)
                   if c in source_df.columns and c in target_df.columns]

    if not common_cols:
        return {
            "check_name": "hash_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "No common columns available for hashing"})
        }

    # Generate normalized SHA-256 hash columns
    src_hashed = build_normalized_row_hash(
        source_df, hash_columns=common_cols, output_hash_column="row_hash_sha256"
    )
    tgt_hashed = build_normalized_row_hash(
        target_df, hash_columns=common_cols, output_hash_column="row_hash_sha256"
    )

    valid_pks = [pk for pk in (primary_keys or []) if pk in common_cols]

    if valid_pks:
        # Key-aligned hash comparison: Join on primary keys and compare hash digests
        src_sub = src_hashed.select(valid_pks + ["row_hash_sha256"]).withColumnRenamed("row_hash_sha256", "src_hash")
        tgt_sub = tgt_hashed.select(valid_pks + ["row_hash_sha256"]).withColumnRenamed("row_hash_sha256", "tgt_hash")

        joined = src_sub.join(tgt_sub, on=valid_pks, how="inner")
        mismatched_hashes_df = joined.filter(col("src_hash") != col("tgt_hash"))

        mismatched_count = mismatched_hashes_df.count()
        rows_compared = joined.count()

        sample_mismatches = [r.asDict() for r in mismatched_hashes_df.limit(20).collect()]
    else:
        # Keyless comparison: Take set differences of distinct hashes across the entire dataset
        src_hashes = src_hashed.select("row_hash_sha256").distinct()
        tgt_hashes = tgt_hashed.select("row_hash_sha256").distinct()

        missing_hashes = src_hashes.subtract(tgt_hashes).count()
        extra_hashes = tgt_hashes.subtract(src_hashes).count()

        mismatched_count = missing_hashes + extra_hashes
        rows_compared = max(src_hashes.count(), tgt_hashes.count())
        sample_mismatches = []

    status = "PASS" if mismatched_count == 0 else "FAIL"

    details = {
        "columns_hashed": common_cols,
        "primary_keys_aligned": valid_pks,
        "hash_mismatch_count": mismatched_count,
        "sample_mismatched_keys": sample_mismatches
    }

    return {
        "check_name": "hash_check",
        "status": status,
        "discrepancy_count": mismatched_count,
        "rows_compared": rows_compared,
        "details_json": json.dumps(details)
    }
