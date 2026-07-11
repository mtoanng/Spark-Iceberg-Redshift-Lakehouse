"""
Instacart Lakehouse Configuration
AWS S3 (storage) + MongoDB (metadata) + DuckDB (query engine)
"""

import os
from pathlib import Path

# =============================================================================
# Project Paths
# =============================================================================
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw" / "instacart"
DATA_PROCESSED_DIR = DATA_DIR / "processed"

# =============================================================================
# AWS S3 Configuration (Lakehouse Storage)
# =============================================================================
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "instacart-lakehouse")

# S3 Paths
S3_RAW_PREFIX = "raw/instacart"
S3_BRONZE_PREFIX = "bronze"
S3_SILVER_PREFIX = "silver"
S3_GOLD_PREFIX = "gold"

# Full S3 URIs (s3a:// for Spark, s3:// for DuckDB)
S3_RAW_PATH = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}"
S3_BRONZE_PATH = f"s3a://{S3_BUCKET}/{S3_BRONZE_PREFIX}"
S3_SILVER_PATH = f"s3a://{S3_BUCKET}/{S3_SILVER_PREFIX}"
S3_GOLD_PATH = f"s3://{S3_BUCKET}/{S3_GOLD_PREFIX}"  # DuckDB reads with s3://

# =============================================================================
# Instacart Dataset Files
# =============================================================================
INSTACART_FILES = {
    "orders": "orders.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv"
}

# Expected row counts (for validation)
EXPECTED_ROW_COUNTS = {
    "orders": 3_421_083,
    "order_products_prior": 32_434_489,
    "order_products_train": 1_384_617,
    "products": 49_688,
    "aisles": 134,
    "departments": 21
}

# =============================================================================
# Apache Spark Configuration (Spark OSS — local dev / EC2 deploy)
# =============================================================================
SPARK_APP_NAME = "Instacart-Lakehouse"

# AWS Credentials (read from environment)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Spark Configs for S3 + Iceberg
SPARK_CONFIGS = {
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Shuffle Partitions (tuned for 1.3GB dataset)
    "spark.sql.shuffle.partitions": "50",
    "spark.default.parallelism": "8",
    
    # Broadcast Join Threshold (10MB)
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
    
    # Iceberg Catalog (pointing to S3)
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": S3_BRONZE_PATH,
    
    # S3 Configurations
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
    "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
    
    # S3 Performance Tuning
    "spark.hadoop.fs.s3a.connection.maximum": "100",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "128M",
    
    # Compression
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.io.compression.codec": "snappy",
}

# =============================================================================
# Apache Iceberg Configuration
# =============================================================================
ICEBERG_WAREHOUSE_PATH = S3_BRONZE_PATH

# Iceberg Table Names
ICEBERG_TABLES = {
    "bronze": {
        "orders": "iceberg.bronze.orders",
        "order_products_prior": "iceberg.bronze.order_products_prior",
        "order_products_train": "iceberg.bronze.order_products_train",
        "products": "iceberg.bronze.products",
        "aisles": "iceberg.bronze.aisles",
        "departments": "iceberg.bronze.departments"
    },
    "silver": {
        "orders_enriched": "iceberg.silver.orders_enriched",
        "order_products_enriched": "iceberg.silver.order_products_enriched",
        "products_hierarchy": "iceberg.silver.products_hierarchy",
        "user_order_summary": "iceberg.silver.user_order_summary"
    }
}

# Iceberg Table Properties
ICEBERG_TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "snappy",
    "commit.manifest.min-count-to-merge": "5"
}

# =============================================================================
# DuckDB Configuration (Query Engine for Gold Layer)
# =============================================================================
DUCKDB_DATABASE = "warehouse.db"  # Embedded DuckDB database
DUCKDB_MEMORY_LIMIT = "2GB"
DUCKDB_THREADS = 4

# =============================================================================
# dbt Configuration
# =============================================================================
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_instacart"
DBT_PROFILES_DIR = Path.home() / ".dbt"
DBT_TARGET = "prod"

# =============================================================================
# MongoDB Configuration (Metadata Catalog Store)
# =============================================================================
# MongoDB acts as a metadata catalog (like Unity Catalog/Hive Metastore)
# Stores: dataset schemas, statistics, lineage, quality scores, tags
# Does NOT store business data (business data stays in Iceberg)
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = "instacart_metadata"
MONGODB_COLLECTIONS = {
    "datasets": "datasets",           # Dataset metadata
    "schemas": "schemas",             # Schema definitions
    "statistics": "statistics",       # Dataset statistics
    "quality": "quality_metrics",     # Quality scores
    "lineage": "lineage"              # Data lineage tracking
}

# =============================================================================
# Airflow Configuration
# =============================================================================
AIRFLOW_DAG_ID = "instacart_lakehouse_pipeline"
AIRFLOW_SCHEDULE = "@weekly"

# =============================================================================
# Data Quality Configuration
# =============================================================================
DATA_QUALITY_RULES = {
    "orders": {
        "required_columns": ["order_id", "user_id", "order_number"],
        "unique_keys": ["order_id"],
        "not_null_columns": ["order_id", "user_id"]
    },
    "order_products": {
        "required_columns": ["order_id", "product_id"],
        "unique_keys": ["order_id", "product_id"],
        "not_null_columns": ["order_id", "product_id"]
    },
    "products": {
        "required_columns": ["product_id", "product_name"],
        "unique_keys": ["product_id"],
        "not_null_columns": ["product_id"]
    }
}

# =============================================================================
# Helper Functions
# =============================================================================
def get_s3_path(layer: str, table_name: str = "") -> str:
    """Get S3 path for a specific layer"""
    base_paths = {
        "raw": S3_RAW_PATH,
        "bronze": S3_BRONZE_PATH,
        "silver": S3_SILVER_PATH
    }
    base = base_paths.get(layer)
    if not base:
        raise ValueError(f"Invalid layer: {layer}")
    return f"{base}/{table_name}" if table_name else base


def get_iceberg_table_name(layer: str, table: str) -> str:
    """Get fully qualified Iceberg table name"""
    if layer not in ICEBERG_TABLES:
        raise ValueError(f"Invalid layer: {layer}")
    if table not in ICEBERG_TABLES[layer]:
        raise ValueError(f"Invalid table: {table} for layer: {layer}")
    return ICEBERG_TABLES[layer][table]


def get_spark_configs() -> dict:
    """Get Spark configuration dictionary"""
    return SPARK_CONFIGS.copy()


if __name__ == "__main__":
    # Print configuration summary
    print("=" * 80)
    print("Instacart Lakehouse Configuration - AWS + MongoDB + DuckDB")
    print("=" * 80)
    print(f"AWS S3 Bucket: {S3_BUCKET}")
    print(f"AWS Region: {AWS_REGION}")
    print(f"\nS3 Paths:")
    print(f"  Raw:    {S3_RAW_PATH}")
    print(f"  Bronze: {S3_BRONZE_PATH}")
    print(f"  Silver: {S3_SILVER_PATH}")
    print(f"\nMetadata Catalog:")
    print(f"  MongoDB URI: {MONGODB_URI}")
    print(f"  Database: {MONGODB_DATABASE}")
    print(f"\nQuery Engine:")
    print(f"  DuckDB (embedded)")
    print("=" * 80)
