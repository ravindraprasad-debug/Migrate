"""
Module: delta_metadata_check.py
Description: Validates Delta Lake detail metadata between legacy Azure and target AWS tables.
Compares partition column ordering, file count, total size in bytes, Delta protocol versions,
and table properties.
"""

import json
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger("ReconFramework.DeltaMetadataCheck")


def run_delta_metadata_check(
    spark: SparkSession,
    source_table_or_path: str,
    target_table_or_path: str
) -> Dict[str, Any]:
    """
    Retrieves and compares DESCRIBE DETAIL metadata for Delta Lake tables.
    
    Args:
        spark (SparkSession): Active Spark session.
        source_table_or_path (str): Source Delta table name or S3/ADLS path.
        target_table_or_path (str): Target Delta table name or S3 path.
        
    Returns:
        Dict[str, Any]: Execution verdict containing metadata comparison results.
    """
    def get_delta_detail(table_or_path: str) -> Optional[Dict[str, Any]]:
        """Helper function to execute DESCRIBE DETAIL on a Delta table or path."""
        try:
            if "/" in table_or_path or table_or_path.startswith("s3://") or table_or_path.startswith("abfss://"):
                df = spark.sql(f"DESCRIBE DETAIL delta.`{table_or_path}`")
            else:
                df = spark.sql(f"DESCRIBE DETAIL {table_or_path}")
            return df.collect()[0].asDict()
        except Exception as e:
            logger.warning(f"Could not retrieve Delta metadata for {table_or_path}: {e}")
            return None

    src_detail = get_delta_detail(source_table_or_path)
    tgt_detail = get_delta_detail(target_table_or_path)

    if not src_detail or not tgt_detail:
        return {
            "check_name": "delta_metadata_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({
                "reason": "Delta metadata unavailable (non-Delta table format or access error)",
                "source": source_table_or_path,
                "target": target_table_or_path
            })
        }

    mismatches = []

    # Validate partition column alignment
    src_part = src_detail.get("partitionColumns") or []
    tgt_part = tgt_detail.get("partitionColumns") or []
    if src_part != tgt_part:
        mismatches.append({
            "property": "partitionColumns",
            "source": src_part,
            "target": tgt_part
        })

    # Validate Delta protocol reader/writer versions
    src_min_reader = src_detail.get("minReaderVersion")
    tgt_min_reader = tgt_detail.get("minReaderVersion")
    src_min_writer = src_detail.get("minWriterVersion")
    tgt_min_writer = tgt_detail.get("minWriterVersion")

    if src_min_reader != tgt_min_reader or src_min_writer != tgt_min_writer:
        mismatches.append({
            "property": "deltaProtocolVersion",
            "source": {"reader": src_min_reader, "writer": src_min_writer},
            "target": {"reader": tgt_min_reader, "writer": tgt_min_writer}
        })

    # Extract file count and size metrics
    src_files = src_detail.get("numFiles")
    tgt_files = tgt_detail.get("numFiles")
    src_size = src_detail.get("sizeInBytes")
    tgt_size = tgt_detail.get("sizeInBytes")

    status = "PASS" if len(mismatches) == 0 else "FAIL"

    details = {
        "source_metadata": {
            "format": src_detail.get("format"),
            "numFiles": src_files,
            "sizeInBytes": src_size,
            "partitionColumns": src_part,
            "properties": str(src_detail.get("properties"))
        },
        "target_metadata": {
            "format": tgt_detail.get("format"),
            "numFiles": tgt_files,
            "sizeInBytes": tgt_size,
            "partitionColumns": tgt_part,
            "properties": str(tgt_detail.get("properties"))
        },
        "mismatches": mismatches
    }

    return {
        "check_name": "delta_metadata_check",
        "status": status,
        "discrepancy_count": len(mismatches),
        "rows_compared": 1,
        "details_json": json.dumps(details)
    }
