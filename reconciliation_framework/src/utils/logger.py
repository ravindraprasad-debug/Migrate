"""
Module: logger.py
Description: Provides structured console logging and telemetry ingestion into Delta Lake audit tables
(reconciliation_audit_log and reconciliation_summary_report).
"""

import os
import logging
import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

logger = logging.getLogger("ReconFramework.AuditLogger")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


# Schema definition for individual check audit logs
AUDIT_SCHEMA = StructType([
    StructField("execution_id", StringType(), True),
    StructField("run_timestamp", TimestampType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("cluster_id", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("status", StringType(), True),  # Values: PASS, FAIL, ERROR, SKIPPED
    StructField("rows_compared", LongType(), True),
    StructField("discrepancy_count", LongType(), True),
    StructField("execution_duration_sec", DoubleType(), True),
    StructField("details_json", StringType(), True),
    StructField("executed_by", StringType(), True)
])


# Schema definition for high-level table reconciliation summary logs
SUMMARY_SCHEMA = StructType([
    StructField("execution_id", StringType(), True),
    StructField("run_timestamp", TimestampType(), True),
    StructField("table_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("overall_status", StringType(), True),  # Values: PASS, FAIL, ERROR
    StructField("source_row_count", LongType(), True),
    StructField("target_row_count", LongType(), True),
    StructField("schema_status", StringType(), True),
    StructField("count_status", StringType(), True),
    StructField("null_dup_status", StringType(), True),
    StructField("pk_status", StringType(), True),
    StructField("aggregate_status", StringType(), True),
    StructField("hash_status", StringType(), True),
    StructField("row_diff_status", StringType(), True),
    StructField("business_rule_status", StringType(), True),
    StructField("partition_status", StringType(), True),
    StructField("delta_meta_status", StringType(), True),
    StructField("incremental_status", StringType(), True),
    StructField("error_table_status", StringType(), True),
    StructField("failed_checks_count", LongType(), True),
    StructField("execution_duration_sec", DoubleType(), True)
])


class AuditLogger:
    """
    Handles structured logging and manages persistent audit records in Delta Lake tables.
    """

    def __init__(
        self,
        spark: SparkSession,
        audit_table_name: str = "gold_governance.reconciliation_audit_log",
        summary_table_name: str = "gold_governance.reconciliation_summary_report",
        environment: str = "AWS_PRODUCTION"
    ):
        """
        Initializes AuditLogger instance and creates target audit Delta tables if missing.
        
        Args:
            spark (SparkSession): Active Spark Session.
            audit_table_name (str): Catalog table for granular check execution logs.
            summary_table_name (str): Catalog table for table-level summary logs.
            environment (str): Deployment environment name.
        """
        self.spark = spark
        self.audit_table_name = audit_table_name
        self.summary_table_name = summary_table_name
        self.environment = environment
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """
        Creates target Delta Lake audit tables if they do not already exist.
        Handles missing database / catalog schema gracefully.
        """
        try:
            # Create detailed check audit table
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.audit_table_name} ("
                           f"execution_id STRING, run_timestamp TIMESTAMP, notebook_name STRING, "
                           f"cluster_id STRING, environment STRING, table_name STRING, layer STRING, "
                           f"check_name STRING, status STRING, rows_compared LONG, discrepancy_count LONG, "
                           f"execution_duration_sec DOUBLE, details_json STRING, executed_by STRING) "
                           f"USING delta")
            
            # Create table summary report table
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.summary_table_name} ("
                           f"execution_id STRING, run_timestamp TIMESTAMP, table_name STRING, layer STRING, "
                           f"overall_status STRING, source_row_count LONG, target_row_count LONG, "
                           f"schema_status STRING, count_status STRING, null_dup_status STRING, "
                           f"pk_status STRING, aggregate_status STRING, hash_status STRING, "
                           f"row_diff_status STRING, business_rule_status STRING, partition_status STRING, "
                           f"delta_meta_status STRING, incremental_status STRING, error_table_status STRING, "
                           f"failed_checks_count LONG, execution_duration_sec DOUBLE) "
                           f"USING delta")
        except Exception as e:
            logger.warning(f"Could not initialize audit Delta tables ({e}). Audit records will log to console.")

    def log_check_result(
        self,
        execution_id: str,
        notebook_name: str,
        table_name: str,
        layer: str,
        check_name: str,
        status: str,
        rows_compared: int = 0,
        discrepancy_count: int = 0,
        duration_sec: float = 0.0,
        details_json: str = "{}",
        executed_by: str = ""
    ):
        """
        Logs individual check execution results to console and appends a record to the audit Delta table.
        """
        log_msg = (
            f"Table: {table_name} | Layer: {layer} | Check: {check_name} | "
            f"Status: {status} | Rows: {rows_compared:,} | Discrepancies: {discrepancy_count:,} | Duration: {duration_sec:.2f}s"
        )
        if status == "PASS":
            logger.info(log_msg)
        elif status == "FAIL":
            logger.error(log_msg)
        else:
            logger.warning(log_msg)

        try:
            record = [(
                execution_id,
                datetime.datetime.now(),
                notebook_name,
                os.getenv("DB_CLUSTER_ID", "local_cluster"),
                self.environment,
                table_name,
                layer,
                check_name,
                status,
                int(rows_compared),
                int(discrepancy_count),
                float(duration_sec),
                str(details_json),
                executed_by or os.getenv("USER", "databricks_user")
            )]
            df = self.spark.createDataFrame(record, schema=AUDIT_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(self.audit_table_name)
        except Exception as e:
            logger.warning(f"Failed to append record to audit Delta table: {e}")

    def log_table_summary(self, summary_dict: Dict[str, Any]):
        """
        Appends overall table reconciliation summary results to the summary Delta table.
        """
        try:
            record = [(
                summary_dict.get("execution_id", ""),
                datetime.datetime.now(),
                summary_dict.get("table_name", ""),
                summary_dict.get("layer", ""),
                summary_dict.get("overall_status", "UNKNOWN"),
                int(summary_dict.get("source_row_count", 0)),
                int(summary_dict.get("target_row_count", 0)),
                summary_dict.get("schema_status", "N/A"),
                summary_dict.get("count_status", "N/A"),
                summary_dict.get("null_dup_status", "N/A"),
                summary_dict.get("pk_status", "N/A"),
                summary_dict.get("aggregate_status", "N/A"),
                summary_dict.get("hash_status", "N/A"),
                summary_dict.get("row_diff_status", "N/A"),
                summary_dict.get("business_rule_status", "N/A"),
                summary_dict.get("partition_status", "N/A"),
                summary_dict.get("delta_meta_status", "N/A"),
                summary_dict.get("incremental_status", "N/A"),
                summary_dict.get("error_table_status", "N/A"),
                int(summary_dict.get("failed_checks_count", 0)),
                float(summary_dict.get("execution_duration_sec", 0.0))
            )]
            df = self.spark.createDataFrame(record, schema=SUMMARY_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(self.summary_table_name)
        except Exception as e:
            logger.warning(f"Failed to append summary to summary Delta table: {e}")
