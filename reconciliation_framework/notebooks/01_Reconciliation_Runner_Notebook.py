# Databricks notebook source
# MAGIC %md
# MAGIC # AWS Databricks Data Migration Reconciliation Runner
# MAGIC 
# MAGIC This notebook executes configuration-driven data reconciliation between legacy Azure Databricks data sources and target AWS Databricks Delta tables.
# MAGIC Validates Schema, Total/Partition Counts, Nulls/Duplicates, Primary Key set differences, Aggregates, SHA-256 Hashes, Row-Level Differencing, Business Domain Rules, Delta Lake Metadata, Incremental CDC loads, and Error/Dead-letter tables.

# COMMAND ----------

import os
import sys
import json

# Append repository root directory to Python path
sys.path.append(os.path.abspath(".."))

from src.utils.spark_session import get_spark_session, get_dbutils
from src.utils.logger import AuditLogger
from src.framework.recon_engine import ReconciliationEngine
from src.framework.report_generator import generate_summary_dataframe, generate_html_dashboard, generate_csv_report

# Initialize Spark Session and DBUtils
spark = get_spark_session("AWS_Databricks_Reconciliation_Runner")
dbutils = get_dbutils(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Interactive Widget Setup

# COMMAND ----------

# Register Databricks text widgets for runtime parameters
dbutils.widgets.text("env", "AWS_PRODUCTION", "Environment")
dbutils.widgets.text("config_path", "../config/recon_config.json", "Config JSON Path")
dbutils.widgets.text("table_filter", "", "Table Filter (optional)")
dbutils.widgets.text("audit_table", "gold_governance.reconciliation_audit_log", "Audit Delta Table")
dbutils.widgets.text("summary_table", "gold_governance.reconciliation_summary_report", "Summary Delta Table")

# Extract widget parameter values
env = dbutils.widgets.get("env")
config_path = dbutils.widgets.get("config_path")
table_filter = dbutils.widgets.get("table_filter")
audit_table = dbutils.widgets.get("audit_table")
summary_table = dbutils.widgets.get("summary_table")

print(f"Executing Reconciliation Engine | Environment: {env} | Config Path: {config_path} | Filter: '{table_filter}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration Loading and Reconciliation Execution

# COMMAND ----------

# Load JSON configuration payload
if os.path.exists(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = json.load(f)
else:
    print(f"Config file not found at path '{config_path}'. Utilizing fallback configuration.")
    config_data = {
        "audit_table": audit_table,
        "summary_report_table": summary_table,
        "tables": []
    }

# Override audit table parameters from widget inputs
config_data["audit_table"] = audit_table
config_data["summary_report_table"] = summary_table

# Instantiate Audit Logger and Reconciliation Engine
audit_logger = AuditLogger(
    spark,
    audit_table_name=audit_table,
    summary_table_name=summary_table,
    environment=env
)

engine = ReconciliationEngine(
    spark=spark,
    config=config_data,
    audit_logger=audit_logger,
    notebook_name="01_Reconciliation_Runner_Notebook"
)

# Run reconciliation pipeline across configured tables
summaries = engine.run_all(table_filter=table_filter if table_filter else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary DataFrame Output

# COMMAND ----------

# Build formatted PySpark summary DataFrame for Databricks UI display
summary_df = generate_summary_dataframe(spark, summaries)
try:
    display(summary_df)
except NameError:
    summary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visual Reconciliation Dashboard Output

# COMMAND ----------

# Generate and display HTML dashboard
html_report = generate_html_dashboard(summaries, title=f"Data Migration Reconciliation Dashboard ({env})")

try:
    displayHTML(html_report)
except NameError:
    report_output_path = "reconciliation_report.html"
    with open(report_output_path, "w", encoding="utf-8") as f:
        f.write(html_report)
    print(f"HTML Dashboard written to {report_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Run Verdict

# COMMAND ----------

failed_tables = [s for s in summaries if s.get("overall_status") in ["FAIL", "ERROR"]]

if failed_tables:
    print(f"WARNING: Reconciliation completed with {len(failed_tables)} table failures.")
    for ft in failed_tables:
        print(f" - Table '{ft.get('table_name')}' (Layer: {ft.get('layer')}) Status: {ft.get('overall_status')}")
else:
    print("SUCCESS: All tables reconciled with 100% parity across Azure and AWS pipelines.")
