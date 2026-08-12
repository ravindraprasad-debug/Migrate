"""
Module: recon_engine.py
Description: Configuration-driven, layered data reconciliation pipeline orchestrator.
Executes cheap, high-signal checks first before running expensive row-level operations.
Provides per-table fault tolerance to prevent single table failures from aborting the run.
"""

import json
import time
import uuid
import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame

from src.utils.logger import AuditLogger
from src.utils.spark_session import translate_adls_to_s3
from src.checks.schema_check import run_schema_check
from src.checks.count_check import run_count_check
from src.checks.null_duplicate_check import run_null_duplicate_check
from src.checks.primary_key_check import run_primary_key_check
from src.checks.aggregate_check import run_aggregate_check
from src.checks.hash_check import run_hash_check
from src.checks.row_diff_check import run_row_diff_check
from src.checks.business_rule_check import run_business_rule_check
from src.checks.delta_metadata_check import run_delta_metadata_check
from src.checks.incremental_check import run_incremental_check
from src.checks.error_table_check import run_error_table_check

logger = logging.getLogger("ReconFramework.Engine")


class ReconciliationEngine:
    """
    Core engine managing multi-table reconciliation pipelines driven by JSON configuration.
    """

    # Enforced fail-fast execution hierarchy (cheap checks run first)
    CHECK_EXECUTION_ORDER = [
        "schema",
        "count",
        "null_duplicate",
        "primary_key",
        "aggregate",
        "hash",
        "row_diff",
        "business_rules",
        "partition",
        "delta_metadata",
        "incremental",
        "error_table"
    ]

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        audit_logger: Optional[AuditLogger] = None,
        notebook_name: str = "Reconciliation_Runner_Notebook"
    ):
        """
        Initializes ReconciliationEngine with Spark session and configuration payload.
        
        Args:
            spark (SparkSession): Active Spark Session.
            config (Dict[str, Any]): Parsed JSON configuration payload.
            audit_logger (Optional[AuditLogger]): AuditLogger instance for telemetry logging.
            notebook_name (str): Calling notebook name.
        """
        self.spark = spark
        self.config = config
        self.notebook_name = notebook_name
        self.audit_logger = audit_logger or AuditLogger(
            spark,
            audit_table_name=config.get("audit_table", "gold_governance.reconciliation_audit_log"),
            summary_table_name=config.get("summary_report_table", "gold_governance.reconciliation_summary_report")
        )
        self.execution_id = str(uuid.uuid4())

    def _load_dataframe(self, table_or_path: str) -> Optional[DataFrame]:
        """
        Loads PySpark DataFrame from Delta catalog table or S3/ADLS storage path.
        
        Args:
            table_or_path (str): Catalog table identifier or S3/ADLS storage path.
            
        Returns:
            Optional[DataFrame]: Loaded PySpark DataFrame or None if loading fails.
        """
        try:
            translated = translate_adls_to_s3(table_or_path)
            if translated.startswith("s3://") or translated.startswith("abfss://") or "/" in translated:
                return self.spark.read.format("delta").load(translated)
            else:
                return self.spark.table(translated)
        except Exception as e:
            logger.error(f"Error loading target table/path '{table_or_path}': {e}")
            return None

    def reconcile_table(self, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes all configured reconciliation checks for a single table.
        Applies layered execution order and logs granular check telemetry.
        
        Args:
            table_config (Dict[str, Any]): Configuration dictionary for the specific table.
            
        Returns:
            Dict[str, Any]: Overall table reconciliation summary dictionary.
        """
        table_name = table_config.get("table_name", "unknown_table")
        layer = table_config.get("layer", "General")
        checks_to_run = table_config.get("compare", self.CHECK_EXECUTION_ORDER)
        pk = table_config.get("pk", [])
        if isinstance(pk, str):
            pk = [pk]

        start_time_all = time.time()
        logger.info(f"Starting Reconciliation for Table: '{table_name}' (Layer: {layer})")

        # Load Source & Target DataFrames
        src_path = table_config.get("source_path_or_name")
        tgt_path = table_config.get("target_path_or_name")

        source_df = self._load_dataframe(src_path)
        target_df = self._load_dataframe(tgt_path)

        if source_df is None or target_df is None:
            err_msg = f"Failed to load DataFrames. Source: {src_path}, Target: {tgt_path}"
            logger.error(err_msg)
            self.audit_logger.log_check_result(
                execution_id=self.execution_id,
                notebook_name=self.notebook_name,
                table_name=table_name,
                layer=layer,
                check_name="table_load",
                status="ERROR",
                details_json=json.dumps({"error": err_msg})
            )
            return {
                "execution_id": self.execution_id,
                "table_name": table_name,
                "layer": layer,
                "overall_status": "ERROR",
                "source_row_count": 0,
                "target_row_count": 0,
                "failed_checks_count": 1,
                "execution_duration_sec": time.time() - start_time_all
            }

        src_count = source_df.count()
        tgt_count = target_df.count()

        results_by_check = {}
        failed_checks_count = 0

        # Execute checks in specified order
        for check_name in self.CHECK_EXECUTION_ORDER:
            if check_name not in checks_to_run and check_name != "partition":
                continue

            if check_name == "partition" and "partition" not in checks_to_run:
                continue

            chk_start = time.time()
            res = None

            try:
                if check_name == "schema":
                    res = run_schema_check(source_df, target_df)

                elif check_name == "count":
                    partition_cols = table_config.get("partition_columns", [])
                    res = run_count_check(source_df, target_df, partition_columns=partition_cols)

                elif check_name == "null_duplicate":
                    crit_cols = table_config.get("critical_columns", source_df.columns)
                    res = run_null_duplicate_check(source_df, target_df, critical_columns=crit_cols, primary_keys=pk)

                elif check_name == "primary_key":
                    res = run_primary_key_check(source_df, target_df, primary_keys=pk)

                elif check_name == "aggregate":
                    agg_cols = table_config.get("aggregate_columns", [])
                    tol = table_config.get("tolerance", 0.0)
                    res = run_aggregate_check(source_df, target_df, aggregate_columns=agg_cols, tolerance=tol)

                elif check_name == "hash":
                    res = run_hash_check(source_df, target_df, primary_keys=pk)

                elif check_name == "row_diff":
                    res = run_row_diff_check(source_df, target_df, primary_keys=pk)

                elif check_name == "business_rules":
                    rules = table_config.get("business_rules", [])
                    res = run_business_rule_check(target_df, source_df=source_df, business_rules=rules)

                elif check_name == "delta_metadata":
                    res = run_delta_metadata_check(self.spark, src_path, tgt_path)

                elif check_name == "incremental":
                    wm_col = table_config.get("watermark_column", "updated_at")
                    lookback = table_config.get("incremental_lookback_days", 1)
                    res = run_incremental_check(source_df, target_df, watermark_column=wm_col, lookback_days=lookback, primary_keys=pk)

                elif check_name == "error_table":
                    err_src_path = table_config.get("error_table_source")
                    err_tgt_path = table_config.get("error_table_target")
                    err_src_df = self._load_dataframe(err_src_path) if err_src_path else None
                    err_tgt_df = self._load_dataframe(err_tgt_path) if err_tgt_path else None
                    res = run_error_table_check(err_src_df, err_tgt_df)

            except Exception as ex:
                logger.error(f"Execution error on check '{check_name}' for table '{table_name}': {ex}")
                res = {
                    "check_name": check_name,
                    "status": "ERROR",
                    "discrepancy_count": 1,
                    "rows_compared": 0,
                    "details_json": json.dumps({"exception": str(ex)})
                }

            if res:
                duration = time.time() - chk_start
                results_by_check[check_name] = res
                status = res.get("status", "UNKNOWN")

                if status in ["FAIL", "ERROR"]:
                    failed_checks_count += 1

                # Log check result to audit log
                self.audit_logger.log_check_result(
                    execution_id=self.execution_id,
                    notebook_name=self.notebook_name,
                    table_name=table_name,
                    layer=layer,
                    check_name=check_name,
                    status=status,
                    rows_compared=res.get("rows_compared", 0),
                    discrepancy_count=res.get("discrepancy_count", 0),
                    duration_sec=duration,
                    details_json=res.get("details_json", "{}")
                )

        overall_status = "PASS" if failed_checks_count == 0 else "FAIL"
        total_duration = time.time() - start_time_all

        table_summary = {
            "execution_id": self.execution_id,
            "table_name": table_name,
            "layer": layer,
            "overall_status": overall_status,
            "source_row_count": src_count,
            "target_row_count": tgt_count,
            "schema_status": results_by_check.get("schema", {}).get("status", "N/A"),
            "count_status": results_by_check.get("count", {}).get("status", "N/A"),
            "null_dup_status": results_by_check.get("null_duplicate", {}).get("status", "N/A"),
            "pk_status": results_by_check.get("primary_key", {}).get("status", "N/A"),
            "aggregate_status": results_by_check.get("aggregate", {}).get("status", "N/A"),
            "hash_status": results_by_check.get("hash", {}).get("status", "N/A"),
            "row_diff_status": results_by_check.get("row_diff", {}).get("status", "N/A"),
            "business_rule_status": results_by_check.get("business_rules", {}).get("status", "N/A"),
            "partition_status": results_by_check.get("count", {}).get("status", "N/A"),
            "delta_meta_status": results_by_check.get("delta_metadata", {}).get("status", "N/A"),
            "incremental_status": results_by_check.get("incremental", {}).get("status", "N/A"),
            "error_table_status": results_by_check.get("error_table", {}).get("status", "N/A"),
            "failed_checks_count": failed_checks_count,
            "execution_duration_sec": total_duration,
            "check_details": results_by_check
        }

        # Log table summary record
        self.audit_logger.log_table_summary(table_summary)
        return table_summary

    def run_all(self, table_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Runs reconciliation across all tables defined in the configuration payload.
        
        Args:
            table_filter (Optional[str]): Single table name filter if isolating execution.
            
        Returns:
            List[Dict[str, Any]]: List of table reconciliation summary dictionaries.
        """
        table_configs = self.config.get("tables", [])
        if table_filter:
            table_configs = [t for t in table_configs if t.get("table_name", "").lower() == table_filter.lower()]

        all_summaries = []
        for t_cfg in table_configs:
            try:
                summary = self.reconcile_table(t_cfg)
                all_summaries.append(summary)
            except Exception as e:
                logger.error(f"Fatal exception reconciling table '{t_cfg.get('table_name')}': {e}")
        return all_summaries
