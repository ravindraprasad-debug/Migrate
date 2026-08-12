"""
Module: spark_session.py
Description: Provides utilities for initializing and configuring PySpark sessions,
handling Databricks runtime dbutils integration, fetching secrets, and translating
legacy Azure ADLS (abfss://) paths into target AWS S3 (s3://) paths.
"""

import os
import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

logger = logging.getLogger("ReconFramework.SparkSession")


def get_spark_session(app_name: str = "AWS_Databricks_Reconciliation") -> SparkSession:
    """
    Retrieves an existing PySpark session or builds a new one.
    Configures Delta Lake extensions and catalog settings for both AWS Databricks
    runtime and local execution environments.
    
    Args:
        app_name (str): Name of the Spark application.
        
    Returns:
        SparkSession: Active SparkSession instance.
    """
    try:
        # Retrieve existing active session if available
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        # Build new session with Delta Lake capabilities
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
    return spark


def get_dbutils(spark: SparkSession):
    """
    Retrieves the Databricks DBUtils instance when running on Databricks clusters,
    or returns a fallback mock object when running locally.
    
    Args:
        spark (SparkSession): Current active Spark Session.
        
    Returns:
        DBUtils or MockDBUtils: Utility object for widgets and secret scope access.
    """
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            logger.info("dbutils package not detected. Running in standard Python/PySpark environment.")
            return MockDBUtils()


class MockDBUtils:
    """
    Mock implementation of Databricks DBUtils to allow code execution in local environments
    without throwing NameError exceptions when accessing widgets or secrets.
    """
    class Widgets:
        """Mock widgets class storing key-value pairs in memory."""
        def __init__(self):
            self._widgets = {}

        def text(self, name: str, defaultValue: str, label: str = ""):
            """Registers a text widget with default value."""
            self._widgets[name] = defaultValue

        def get(self, name: str) -> str:
            """Retrieves stored widget value."""
            return self._widgets.get(name, "")

        def removeAll(self):
            """Clears all registered widgets."""
            self._widgets.clear()

    class Secrets:
        """Mock secrets class reading credentials from environment variables."""
        def get(self, scope: str, key: str) -> str:
            """Retrieves secret key from environment variables or mock fallback."""
            return os.getenv(key, f"mock_secret_{key}")

    def __init__(self):
        self.widgets = self.Widgets()
        self.secrets = self.Secrets()


def translate_adls_to_s3(path_or_table: str, path_mapping: Optional[Dict[str, str]] = None) -> str:
    """
    Translates legacy Azure ADLS storage paths (abfss://) into equivalent AWS S3 (s3://) paths.
    
    Args:
        path_or_table (str): Original input path or table identifier.
        path_mapping (Optional[Dict[str, str]]): Explicit dictionary mapping Azure prefixes to AWS prefixes.
        
    Returns:
        str: Rewritten path pointing to AWS S3 storage location.
    """
    if not path_or_table:
        return path_or_table

    # Apply explicit user path mapping overrides if provided
    if path_mapping:
        for azure_prefix, aws_prefix in path_mapping.items():
            if path_or_table.startswith(azure_prefix):
                return path_or_table.replace(azure_prefix, aws_prefix, 1)

    # Standard transformation fallback for abfss:// URIs
    if path_or_table.startswith("abfss://"):
        parts = path_or_table.replace("abfss://", "").split("/", 1)
        container_storage = parts[0]
        subpath = parts[1] if len(parts) > 1 else ""
        container = container_storage.split("@")[0] if "@" in container_storage else container_storage
        return f"s3://databricks-{container}/{subpath}"

    return path_or_table


def get_secret(scope: str, key: str, spark: Optional[SparkSession] = None) -> str:
    """
    Retrieves secret string from Databricks Secret Scope or falls back to OS environment variables.
    
    Args:
        scope (str): Databricks secret scope name.
        key (str): Secret key name.
        spark (Optional[SparkSession]): Active Spark Session.
        
    Returns:
        str: Secret payload value.
    """
    if spark is None:
        spark = get_spark_session()
    dbutils = get_dbutils(spark)
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        logger.warning(f"Unable to fetch secret from Databricks scope '{scope}' key '{key}': {e}. Falling back to OS env.")
        return os.getenv(key, "")
