"""
Module: string_normalizer.py
Description: Enforces character encoding, Unicode NFC normalization, whitespace trimming,
non-printable control character removal, and lowercasing on string columns to guarantee
exact parity when computing SHA-256 row hashes across Azure and AWS Spark environments.
"""

import unicodedata
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, trim, lower, regexp_replace, concat_ws, sha2, coalesce, lit
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def normalize_unicode_nfc(text: Optional[str]) -> Optional[str]:
    """
    User-Defined Function (UDF) to convert string input into Unicode NFC (Canonical Composition) format.
    Prevents false hash mismatches caused by identical visual characters stored under different
    byte sequences (e.g. precomposed characters vs decomposed base character + combining mark).
    
    Args:
        text (Optional[str]): Raw string input.
        
    Returns:
        Optional[str]: NFC normalized string or None if input is None.
    """
    if text is None:
        return None
    return unicodedata.normalize("NFC", str(text))


def apply_string_normalization(
    df: DataFrame,
    columns_to_normalize: Optional[List[str]] = None,
    lowercase: bool = True,
    trim_whitespace: bool = True,
    strip_non_printable: bool = True,
    apply_unicode_nfc: bool = True
) -> DataFrame:
    """
    Applies standard string transformations to specified columns in a PySpark DataFrame.
    
    Processing Steps:
      1. Strip non-printable / control ASCII characters (regex range [\\x00-\\x1F\\x7F-\\x9F]).
      2. Trim leading and trailing whitespace.
      3. Lowercase all text for case-insensitive collation parity.
      4. Normalize Unicode strings to NFC format.
      
    Args:
        df (DataFrame): Input PySpark DataFrame.
        columns_to_normalize (Optional[List[str]]): Target column names. Defaults to all StringType columns.
        lowercase (bool): Convert text to lowercase. Defaults to True.
        trim_whitespace (bool): Trim leading/trailing whitespace. Defaults to True.
        strip_non_printable (bool): Strip non-printable characters. Defaults to True.
        apply_unicode_nfc (bool): Apply Unicode NFC normalization UDF. Defaults to True.
        
    Returns:
        DataFrame: Transformed DataFrame with normalized string columns.
    """
    if columns_to_normalize is None:
        # Automatically discover all StringType columns in schema
        columns_to_normalize = [
            field.name for field in df.schema.fields if field.dataType.typeName() == "string"
        ]

    normalized_df = df
    for c in columns_to_normalize:
        if c not in df.columns:
            continue

        col_expr = col(c)

        # Step 1: Remove control characters
        if strip_non_printable:
            col_expr = regexp_replace(col_expr, r"[\x00-\x1F\x7F-\x9F]", "")

        # Step 2: Trim whitespace
        if trim_whitespace:
            col_expr = trim(col_expr)

        # Step 3: Convert to lowercase
        if lowercase:
            col_expr = lower(col_expr)

        # Step 4: Apply Unicode NFC normalization UDF
        if apply_unicode_nfc:
            col_expr = normalize_unicode_nfc(col_expr)

        normalized_df = normalized_df.withColumn(c, col_expr)

    return normalized_df


def build_normalized_row_hash(
    df: DataFrame,
    hash_columns: Optional[List[str]] = None,
    output_hash_column: str = "row_hash_sha256",
    null_sentinel: str = "__NULL__"
) -> DataFrame:
    """
    Computes a deterministic SHA-256 hash across normalized row attributes.
    Uses concat_ws with double-pipe '||' delimiters and coalesces null values to a sentinel string.
    
    Formula:
        sha2(concat_ws('||', coalesce(c1, '__NULL__'), coalesce(c2, '__NULL__'), ...), 256)
        
    Args:
        df (DataFrame): Input PySpark DataFrame.
        hash_columns (Optional[List[str]]): List of columns to include in hash.
        output_hash_column (str): Target column name for generated SHA-256 hash string.
        null_sentinel (str): Constant string used to represent null values.
        
    Returns:
        DataFrame: PySpark DataFrame augmented with the generated SHA-256 hash column.
    """
    if hash_columns is None:
        # Default to all existing columns except the output hash column itself
        hash_columns = [c for c in df.columns if c != output_hash_column]

    # Pre-normalize target string columns
    normalized_df = apply_string_normalization(df, columns_to_normalize=hash_columns)

    # Cast columns to StringType and coalesce nulls to prevent null propagation
    concat_exprs = [
        coalesce(col(c).cast(StringType()), lit(null_sentinel))
        for c in hash_columns
    ]

    # Append generated SHA-256 digest column
    return normalized_df.withColumn(
        output_hash_column,
        sha2(concat_ws("||", *concat_exprs), 256)
    )
