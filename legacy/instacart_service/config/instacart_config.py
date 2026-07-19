"""Runtime configuration for local scripts.

Terraform remains the source of truth for infrastructure values. Export the
matching uppercase environment variables after `terraform apply`.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional convenience dependency
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent

if load_dotenv:
    load_dotenv(PROJECT_ROOT / ".env")


AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/instacart")
S3_WAREHOUSE_PREFIX = os.getenv("S3_WAREHOUSE_PREFIX", "warehouse")
S3_GOLD_PATH = os.getenv(
    "S3_GOLD_PATH",
    f"s3://{S3_BUCKET}/gold" if S3_BUCKET else None,
)

GLUE_DATABASE = os.getenv("GLUE_DATABASE", "instacart_lakehouse_dev")
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")

# PRODUCTION: MongoDB Atlas (recommendations only)
MONGODB_URI = os.getenv("MONGODB_URI")
if not MONGODB_URI:
    raise ValueError(
        "MONGODB_URI environment variable required. "
        "Use MongoDB Atlas: mongodb+srv://username:password@cluster.mongodb.net/"
    )
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "instacart_ml_warehouse")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse/data/warehouse.db")
# DUCKDB_ROLE_ARN removed - Docker mounts ~/.aws, uses IAM user credentials
USE_GLUE_CATALOG = os.getenv("USE_GLUE_CATALOG", "true").lower() == "true"

INSTACART_FILES = {
    "orders": "orders.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
}

SPARK_CONFIGS = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": f"s3://{S3_BUCKET}/{S3_WAREHOUSE_PREFIX}/"
    if S3_BUCKET
    else "",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
}

ICEBERG_TABLES = {
    "bronze": {
        "orders": "glue_catalog.bronze.orders",
        "products": "glue_catalog.bronze.products",
        "aisles": "glue_catalog.bronze.aisles",
        "departments": "glue_catalog.bronze.departments",
        "order_products_prior": "glue_catalog.bronze.order_products_prior",
        "order_products_train": "glue_catalog.bronze.order_products_train",
    },
    "silver": {
        "orders_enriched": "glue_catalog.silver.orders_enriched",
        "order_products_enriched": "glue_catalog.silver.order_products_enriched",
        "products_hierarchy": "glue_catalog.silver.products_hierarchy",
    },
}
