"""
Module: business_rule_check.py
Description: Evaluates domain constraints and business logic rules (e.g. order_amount >= 0,
status IN ('OPEN', 'CLOSED', 'PENDING'), age >= 18) against PySpark DataFrames using SQL expressions.
"""

import json
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame


def run_business_rule_check(
    target_df: DataFrame,
    source_df: Optional[DataFrame] = None,
    business_rules: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Evaluates business rule constraints against target (and optionally source) DataFrames.
    
    Args:
        target_df (DataFrame): Target PySpark DataFrame to validate.
        source_df (Optional[DataFrame]): Source PySpark DataFrame for comparative rule auditing.
        business_rules (Optional[List[str]]): List of SQL expressions representing domain rules.
        
    Returns:
        Dict[str, Any]: Execution verdict containing pass/fail status and violation details.
    """
    if not business_rules:
        return {
            "check_name": "business_rule_check",
            "status": "SKIPPED",
            "discrepancy_count": 0,
            "rows_compared": 0,
            "details_json": json.dumps({"reason": "No business rules defined in configuration"})
        }

    rule_results = []
    total_violations = 0
    target_total_rows = target_df.count()

    # Evaluate each SQL expression rule
    for rule_sql in business_rules:
        try:
            # Filter rows violating the condition (NOT rule_sql)
            invalid_df = target_df.filter(f"NOT ({rule_sql})")
            violation_count = invalid_df.count()
            total_violations += violation_count

            source_violation_count = 0
            if source_df is not None:
                source_invalid_df = source_df.filter(f"NOT ({rule_sql})")
                source_violation_count = source_invalid_df.count()

            rule_results.append({
                "rule_expression": rule_sql,
                "target_violations": violation_count,
                "source_violations": source_violation_count,
                "status": "PASS" if violation_count == 0 else "FAIL",
                "sample_violating_rows": [r.asDict() for r in invalid_df.limit(5).collect()]
            })
        except Exception as e:
            rule_results.append({
                "rule_expression": rule_sql,
                "error": str(e),
                "status": "ERROR"
            })
            total_violations += 1

    status = "PASS" if total_violations == 0 else "FAIL"

    details = {
        "business_rules_evaluated_count": len(business_rules),
        "total_rule_violations": total_violations,
        "rule_evaluations": rule_results
    }

    return {
        "check_name": "business_rule_check",
        "status": status,
        "discrepancy_count": total_violations,
        "rows_compared": target_total_rows * len(business_rules),
        "details_json": json.dumps(details)
    }
