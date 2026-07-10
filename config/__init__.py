"""
Configuration package for Instacart Lakehouse
AWS S3 + MongoDB (metadata) + DuckDB (query engine)
"""

from .instacart_config import *

__all__ = [
    'S3_BUCKET',
    'S3_RAW_PREFIX',
    'S3_BRONZE_PREFIX',
    'S3_SILVER_PREFIX',
    'S3_RAW_PATH',
    'S3_BRONZE_PATH',
    'S3_SILVER_PATH',
    'AWS_REGION',
    'MONGODB_URI',
    'MONGODB_DATABASE',
    'MONGODB_COLLECTIONS',
    'DUCKDB_DATABASE',
    'SPARK_CONFIGS',
    'ICEBERG_TABLES',
    'INSTACART_FILES',
    'EXPECTED_ROW_COUNTS',
]
