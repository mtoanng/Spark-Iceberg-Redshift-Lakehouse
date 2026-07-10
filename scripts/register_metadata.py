"""
Register metadata for Gold layer tables to MongoDB catalog

Run this script after dbt completes to populate the metadata catalog.

Usage:
    python scripts/register_metadata.py
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.instacart_config import SPARK_CONFIGS, MONGODB_URI, MONGODB_DATABASE
from warehouse.metadata import MetadataStore


def create_spark_session():
    """Create Spark session"""
    builder = SparkSession.builder.appName("Metadata-Registration")
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def get_table_metadata(spark, schema: str, table: str):
    """
    Get metadata for a table from Spark catalog
    
    Returns:
        Dict with table metadata (row count, location, schema)
    """
    full_table_name = f"iceberg.{schema}.{table}"
    
    try:
        # Get table stats
        df = spark.table(full_table_name)
        row_count = df.count()
        
        # Get table location
        table_info = spark.sql(f"DESCRIBE EXTENDED {full_table_name}").collect()
        location = None
        for row in table_info:
            if row.col_name == "Location":
                location = row.data_type
                break
        
        # Get schema
        schema_info = [
            {"name": field.name, "type": str(field.dataType)}
            for field in df.schema.fields
        ]
        
        return {
            "dataset_id": f"{schema}.{table}",
            "schema_name": schema,
            "table_name": table,
            "row_count": row_count,
            "location": location or f"s3://instacart-lakehouse/{schema}/{table}",
            "schema": schema_info,
            "table_format": "iceberg",
            "updated_at": datetime.utcnow()
        }
    except Exception as e:
        print(f"⚠️  Could not get metadata for {full_table_name}: {str(e)}")
        return None


def main():
    """Main registration logic"""
    print("=" * 80)
    print("📝 REGISTERING GOLD LAYER METADATA TO MONGODB")
    print("=" * 80)
    
    # Initialize
    spark = create_spark_session()
    metadata_store = MetadataStore(uri=MONGODB_URI, database=MONGODB_DATABASE)
    
    # Gold layer tables to register
    gold_tables = [
        "dim_user",
        "dim_product",
        "dim_date",
        "fct_order_products"
    ]
    
    registered = 0
    failed = 0
    
    for table in gold_tables:
        print(f"\n📊 Processing: gold.{table}")
        
        try:
            metadata = get_table_metadata(spark, "gold", table)
            if metadata:
                dataset_id = metadata_store.register_dataset(metadata)
                print(f"✅ Registered: {dataset_id} ({metadata['row_count']:,} rows)")
                registered += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Failed to register gold.{table}: {str(e)}")
            failed += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 REGISTRATION SUMMARY")
    print("=" * 80)
    print(f"✅ Registered: {registered}")
    print(f"❌ Failed: {failed}")
    print(f"💾 MongoDB URI: {MONGODB_URI}")
    print(f"💾 Database: {MONGODB_DATABASE}")
    print("=" * 80)
    
    spark.stop()
    metadata_store.close()
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
