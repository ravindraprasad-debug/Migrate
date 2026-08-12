# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation Framework Self-Test Notebook
# MAGIC 
# MAGIC Validates the reconciliation framework check modules against synthetic test fixtures (Known-Good, Known-Bad, Edge Cases)
# MAGIC to guarantee that PASS verdicts are genuine and FAIL verdicts accurately flag injected data defects.

# COMMAND ----------

import os
import sys

# Append repository root directory to Python path
sys.path.append(os.path.abspath(".."))

from src.utils.spark_session import get_spark_session
from src.checks.schema_check import run_schema_check
from src.checks.count_check import run_count_check
from src.checks.null_duplicate_check import run_null_duplicate_check
from src.checks.primary_key_check import run_primary_key_check
from src.checks.aggregate_check import run_aggregate_check
from src.checks.hash_check import run_hash_check
from src.checks.row_diff_check import run_row_diff_check
from src.checks.business_rule_check import run_business_rule_check

# Initialize local Spark session for self-testing
spark = get_spark_session("Framework_Self_Test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Fixture 1: Known-Good Synthetic Data (Must PASS all checks)

# COMMAND ----------

schema_cols = ["customer_id", "name", "city", "salary"]
data_good = [
    (1, "Alice Smith", "Seattle", 120000.0),
    (2, "Bob Jones", "New York", 95000.0),
    (3, "Charlie Brown", "Chicago", 80000.0)
]

df_src_good = spark.createDataFrame(data_good, schema=schema_cols)
df_tgt_good = spark.createDataFrame(data_good, schema=schema_cols)

res_schema = run_schema_check(df_src_good, df_tgt_good)
res_count = run_count_check(df_src_good, df_tgt_good)
res_null_dup = run_null_duplicate_check(df_src_good, df_tgt_good, primary_keys=["customer_id"])
res_pk = run_primary_key_check(df_src_good, df_tgt_good, primary_keys=["customer_id"])
res_agg = run_aggregate_check(df_src_good, df_tgt_good, aggregate_columns=["salary"])
res_hash = run_hash_check(df_src_good, df_tgt_good, primary_keys=["customer_id"])
res_diff = run_row_diff_check(df_src_good, df_tgt_good, primary_keys=["customer_id"])
res_rules = run_business_rule_check(df_tgt_good, business_rules=["salary >= 0", "customer_id > 0"])

assert res_schema["status"] == "PASS", "Schema check failed on known-good fixture."
assert res_count["status"] == "PASS", "Count check failed on known-good fixture."
assert res_null_dup["status"] == "PASS", "Null/Dup check failed on known-good fixture."
assert res_pk["status"] == "PASS", "PK check failed on known-good fixture."
assert res_agg["status"] == "PASS", "Aggregate check failed on known-good fixture."
assert res_hash["status"] == "PASS", "Hash check failed on known-good fixture."
assert res_diff["status"] == "PASS", "Row Diff check failed on known-good fixture."
assert res_rules["status"] == "PASS", "Business Rules check failed on known-good fixture."

print("PASSED: Known-Good Synthetic Fixture Suite (8/8 checks passed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Fixture 2: Known-Bad Synthetic Data (Must FAIL with expected check flags)

# COMMAND ----------

# Target DataFrame with missing row, modified salary value, and business rule violation
data_bad = [
    (1, "Alice Smith", "Seattle", 120000.0),
    (2, "Bob Jones", "New York", -500.0), # Injected negative salary violation & hash mismatch
    # Row 3 (Charlie Brown) is omitted to test row count and primary key set difference failure
]

df_tgt_bad = spark.createDataFrame(data_bad, schema=schema_cols)

bad_count = run_count_check(df_src_good, df_tgt_bad)
bad_pk = run_primary_key_check(df_src_good, df_tgt_bad, primary_keys=["customer_id"])
bad_hash = run_hash_check(df_src_good, df_tgt_bad, primary_keys=["customer_id"])
bad_rules = run_business_rule_check(df_tgt_bad, business_rules=["salary >= 0"])

assert bad_count["status"] == "FAIL", "Count check should have reported FAIL."
assert bad_pk["status"] == "FAIL", "PK check should have reported FAIL."
assert bad_hash["status"] == "FAIL", "Hash check should have reported FAIL."
assert bad_rules["status"] == "FAIL", "Business rules check should have reported FAIL."

print("PASSED: Known-Bad Synthetic Fixture Suite (Correctly flagged count, PK, hash, and rule defects).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Fixture 3: Edge Cases (Empty DataFrames)

# COMMAND ----------

df_empty_src = spark.createDataFrame([], schema=df_src_good.schema)
df_empty_tgt = spark.createDataFrame([], schema=df_tgt_good.schema)

empty_count = run_count_check(df_empty_src, df_empty_tgt)
empty_schema = run_schema_check(df_empty_src, df_empty_tgt)

assert empty_count["status"] == "PASS", "Empty table count comparison should pass when both sides are empty."
assert empty_schema["status"] == "PASS", "Empty table schema comparison should pass."

print("PASSED: Edge Cases Synthetic Suite (Empty tables handled gracefully).")
print("ALL FRAMEWORK SELF-TESTS COMPLETED SUCCESSFULLY.")
