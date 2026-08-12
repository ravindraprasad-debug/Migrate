"""
Module: report_generator.py
Description: Generates output reconciliation reports in multiple formats:
1. Formatted PySpark DataFrames for Databricks display() and UI rendering.
2. CSV summary report string exports.
3. Interactive HTML visual dashboard with key metrics and status badges (strictly no emojis).
"""

import os
import json
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame


def generate_summary_dataframe(spark: SparkSession, summaries: List[Dict[str, Any]]) -> DataFrame:
    """
    Converts list of table reconciliation summaries into a formatted PySpark DataFrame for Databricks display().
    
    Args:
        spark (SparkSession): Active Spark Session.
        summaries (List[Dict[str, Any]]): List of table summary dictionaries.
        
    Returns:
        DataFrame: PySpark DataFrame formatted for display.
    """
    rows = []
    for s in summaries:
        rows.append((
            s.get("table_name", ""),
            s.get("layer", ""),
            s.get("overall_status", ""),
            s.get("source_row_count", 0),
            s.get("target_row_count", 0),
            s.get("schema_status", "N/A"),
            s.get("count_status", "N/A"),
            s.get("null_dup_status", "N/A"),
            s.get("pk_status", "N/A"),
            s.get("aggregate_status", "N/A"),
            s.get("hash_status", "N/A"),
            s.get("row_diff_status", "N/A"),
            s.get("business_rule_status", "N/A"),
            s.get("delta_meta_status", "N/A"),
            s.get("incremental_status", "N/A"),
            s.get("error_table_status", "N/A"),
            round(s.get("execution_duration_sec", 0.0), 2)
        ))

    schema = [
        "Table Name", "Layer", "Overall Status", "Source Rows", "Target Rows",
        "Schema", "Count", "Null/Dup", "Primary Key", "Aggregates", "Hash",
        "Row Diff", "Business Rules", "Delta Metadata", "Incremental", "Error Table", "Duration (s)"
    ]

    return spark.createDataFrame(rows, schema=schema)


def generate_csv_report(summaries: List[Dict[str, Any]]) -> str:
    """
    Generates CSV formatted string summary of reconciliation results.
    
    Args:
        summaries (List[Dict[str, Any]]): List of table summary dictionaries.
        
    Returns:
        str: Comma-separated CSV output.
    """
    header = "Table,Layer,Overall_Status,Source_Rows,Target_Rows,Schema,Count,Hash,Row_Diff,Business_Rules,Duration_Sec\n"
    lines = [header]
    for s in summaries:
        line = (
            f"{s.get('table_name')},{s.get('layer')},{s.get('overall_status')},"
            f"{s.get('source_row_count')},{s.get('target_row_count')},"
            f"{s.get('schema_status')},{s.get('count_status')},{s.get('hash_status')},"
            f"{s.get('row_diff_status')},{s.get('business_rule_status')},"
            f"{round(s.get('execution_duration_sec', 0.0), 2)}\n"
        )
        lines.append(line)
    return "".join(lines)


def generate_html_dashboard(summaries: List[Dict[str, Any]], title: str = "Data Migration Reconciliation Report") -> str:
    """
    Generates an HTML visual dashboard displaying reconciliation metrics and status badges.
    
    Args:
        summaries (List[Dict[str, Any]]): List of table summary dictionaries.
        title (str): Dashboard header title.
        
    Returns:
        str: Complete HTML document string.
    """
    total_tables = len(summaries)
    passed_tables = sum(1 for s in summaries if s.get("overall_status") == "PASS")
    failed_tables = sum(1 for s in summaries if s.get("overall_status") == "FAIL")

    pass_rate = (passed_tables / total_tables * 100) if total_tables > 0 else 0

    table_rows_html = ""
    for s in summaries:
        status = s.get("overall_status", "UNKNOWN")
        badge_cls = "badge-pass" if status == "PASS" else ("badge-fail" if status == "FAIL" else "badge-error")

        table_rows_html += f"""
        <tr>
            <td class="font-bold">{s.get('table_name')}</td>
            <td><span class="layer-tag">{s.get('layer')}</span></td>
            <td><span class="{badge_cls}">{status}</span></td>
            <td>{s.get('source_row_count'):,}</td>
            <td>{s.get('target_row_count'):,}</td>
            <td>{_status_pill(s.get('schema_status'))}</td>
            <td>{_status_pill(s.get('count_status'))}</td>
            <td>{_status_pill(s.get('hash_status'))}</td>
            <td>{_status_pill(s.get('row_diff_status'))}</td>
            <td>{_status_pill(s.get('business_rule_status'))}</td>
            <td>{_status_pill(s.get('incremental_status'))}</td>
            <td>{round(s.get('execution_duration_sec', 0.0), 2)}s</td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8"/>
    <title>{title}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #0f172a; color: #f8fafc; margin: 0; padding: 24px; }}
        .header {{ margin-bottom: 24px; text-align: center; }}
        .header h1 {{ font-size: 28px; color: #38bdf8; margin-bottom: 8px; }}
        .header p {{ color: #94a3b8; font-size: 14px; }}
        .cards {{ display: flex; gap: 16px; justify-content: center; margin-bottom: 32px; }}
        .card {{ background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px 24px; min-width: 140px; text-align: center; }}
        .card-num {{ font-size: 32px; font-weight: bold; margin-top: 4px; }}
        .card-label {{ font-size: 12px; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; }}
        .text-pass {{ color: #4ade80; }}
        .text-fail {{ color: #f87171; }}
        .text-blue {{ color: #38bdf8; }}
        table {{ width: 100%; border-collapse: collapse; background-color: #1e293b; border-radius: 8px; overflow: hidden; font-size: 14px; }}
        th {{ background-color: #0f172a; color: #94a3b8; font-weight: 600; text-align: left; padding: 12px 16px; border-bottom: 1px solid #334155; }}
        td {{ padding: 12px 16px; border-bottom: 1px solid #334155; color: #e2e8f0; }}
        tr:hover {{ background-color: #334155; }}
        .font-bold {{ font-weight: 600; color: #ffffff; }}
        .layer-tag {{ background: #334155; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        .badge-pass {{ background: #15803d; color: #ffffff; padding: 4px 10px; border-radius: 12px; font-weight: bold; font-size: 12px; }}
        .badge-fail {{ background: #b91c1c; color: #ffffff; padding: 4px 10px; border-radius: 12px; font-weight: bold; font-size: 12px; }}
        .badge-error {{ background: #c2410c; color: #ffffff; padding: 4px 10px; border-radius: 12px; font-weight: bold; font-size: 12px; }}
        .pill-pass {{ color: #4ade80; font-weight: bold; }}
        .pill-fail {{ color: #f87171; font-weight: bold; }}
        .pill-na {{ color: #64748b; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <p>Azure Databricks to AWS Databricks Data Migration Validation Summary</p>
    </div>

    <div class="cards">
        <div class="card"><div class="card-label">Total Tables</div><div class="card-num text-blue">{total_tables}</div></div>
        <div class="card"><div class="card-label">Passed</div><div class="card-num text-pass">{passed_tables}</div></div>
        <div class="card"><div class="card-label">Failed</div><div class="card-num text-fail">{failed_tables}</div></div>
        <div class="card"><div class="card-label">Pass Rate</div><div class="card-num text-pass">{pass_rate:.1f}%</div></div>
    </div>

    <table>
        <thead>
            <tr>
                <th>Table Name</th>
                <th>Layer</th>
                <th>Status</th>
                <th>Source Rows</th>
                <th>Target Rows</th>
                <th>Schema</th>
                <th>Count</th>
                <th>Hash</th>
                <th>Row Diff</th>
                <th>Business Rules</th>
                <th>Incremental</th>
                <th>Duration</th>
            </tr>
        </thead>
        <tbody>
            {table_rows_html}
        </tbody>
    </table>
</body>
</html>
"""
    return html


def _status_pill(status: str) -> str:
    """Formats text status pills without emojis."""
    if status == "PASS":
        return '<span class="pill-pass">[PASS]</span>'
    elif status in ["FAIL", "ERROR"]:
        return '<span class="pill-fail">[FAIL]</span>'
    return '<span class="pill-na">[N/A]</span>'
