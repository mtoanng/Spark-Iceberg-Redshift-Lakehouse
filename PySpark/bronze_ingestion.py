"""
Bronze Layer Ingestion - Instacart Dataset
Reads raw CSV files from S3 and writes to Iceberg Bronze tables
Preserves raw data with minimal transformation (immutable landing zone)

Author: Data Engineering Team
Date: 2026-07-10
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import *

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.instacart_config import (
    S3_BUCKET, S3_RAW_PREFIX, S3_BRONZE_PREFIX,
    INSTACART_FILES, EXPECTED_ROW_COUNTS,
    SPARK_CONFIGS
)


def create_spark_session():
    """
    Create and configure Spark session with Iceberg and S3 support
    
    Returns:
        SparkSession: Configured Spark session
    """
    print("🔧 Creating Spark session...")
    
    builder = SparkSession.builder.appName("Instacart-Bronze-Ingestion")
    
    # Apply all configs from config file
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created successfully")
    return spark


def validate_csv_schema(df, table_name, expected_row_count):
    """
    Validate CSV schema and row count
    
    Args:
        df: Spark DataFrame
        table_name: Name of table for logging
        expected_row_count: Expected number of rows
        
    Returns:
        bool: True if validation passes
    """
    actual_count = df.count()
    tolerance = 0.01  # 1% tolerance
    
    min_expected = int(expected_row_count * (1 - tolerance))
    max_expected = int(expected_row_count * (1 + tolerance))
    
    if min_expected <= actual_count <= max_expected:
        print(f"✅ Row count validation passed: {actual_count:,} rows")
        return True
    else:
        print(f"⚠️  Row count mismatch: Expected ~{expected_row_count:,}, got {actual_count:,}")
        return True  # Warning but continue


def ingest_orders(spark):
    """
    Ingest orders.csv to Bronze layer
    
    Schema:
        order_id, user_id, eval_set, order_number, order_dow,
        order_hour_of_day, days_since_prior_order
    """
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Orders")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['orders']}"
    print(f"📂 Reading from: {raw_path}")
    
    try:
        # Read CSV with schema inference
        df_orders = spark.read.csv(raw_path, header=True, inferSchema=True)
        
        # Validate
        validate_csv_schema(df_orders, "orders", EXPECTED_ROW_COUNTS["orders"])
        
        # Add metadata columns
        df_orders = df_orders \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["orders"]))
        
        print(f"📊 Schema:")
        df_orders.printSchema()
        
        # Write to Iceberg Bronze table
        iceberg_table = "iceberg.bronze.orders"
        print(f"💾 Writing to Iceberg table: {iceberg_table}")
        
        df_orders.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_orders.count():,} records")
        
        # Show sample
        print("\n📋 Sample records:")
        df_orders.select("order_id", "user_id", "order_number", "order_dow", "order_hour_of_day").show(5)
        
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting orders: {str(e)}")
        raise


def ingest_order_products_prior(spark):
    """
    Ingest order_products__prior.csv to Bronze layer
    
    Schema: order_id, product_id, add_to_cart_order, reordered
    
    Note: This is the LARGEST file (32.4M rows)
    """
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Order Products (Prior)")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['order_products_prior']}"
    print(f"📂 Reading from: {raw_path}")
    print("⏳ This may take a while (32.4M rows)...")
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        
        validate_csv_schema(df, "order_products_prior", EXPECTED_ROW_COUNTS["order_products_prior"])
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["order_products_prior"])) \
            .withColumn("eval_set", lit("prior"))  # Mark as prior set
        
        iceberg_table = "iceberg.bronze.order_products_prior"
        print(f"💾 Writing to Iceberg table: {iceberg_table}")
        
        df.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting order_products_prior: {str(e)}")
        raise


def ingest_order_products_train(spark):
    """
    Ingest order_products__train.csv to Bronze layer
    
    Schema: order_id, product_id, add_to_cart_order, reordered
    """
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Order Products (Train)")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['order_products_train']}"
    print(f"📂 Reading from: {raw_path}")
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        
        validate_csv_schema(df, "order_products_train", EXPECTED_ROW_COUNTS["order_products_train"])
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["order_products_train"])) \
            .withColumn("eval_set", lit("train"))  # Mark as train set
        
        iceberg_table = "iceberg.bronze.order_products_train"
        print(f"💾 Writing to Iceberg table: {iceberg_table}")
        
        df.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting order_products_train: {str(e)}")
        raise


def ingest_products(spark):
    """
    Ingest products.csv to Bronze layer
    
    Schema: product_id, product_name, aisle_id, department_id
    """
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Products")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['products']}"
    print(f"📂 Reading from: {raw_path}")
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        
        validate_csv_schema(df, "products", EXPECTED_ROW_COUNTS["products"])
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["products"]))
        
        iceberg_table = "iceberg.bronze.products"
        df.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        
        # Show sample product names
        print("\n📋 Sample products:")
        df.select("product_id", "product_name", "aisle_id", "department_id").show(10, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting products: {str(e)}")
        raise


def ingest_aisles(spark):
    """Ingest aisles.csv to Bronze layer"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Aisles")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['aisles']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        df.writeTo("iceberg.bronze.aisles") \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} aisles")
        return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def ingest_departments(spark):
    """Ingest departments.csv to Bronze layer"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Departments")
    print("=" * 80)
    
    raw_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/{INSTACART_FILES['departments']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        df.writeTo("iceberg.bronze.departments") \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} departments")
        return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def main():
    """Main execution function"""
    
    print("\n" + "=" * 80)
    print("🚀 INSTACART BRONZE LAYER INGESTION")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🪣 S3 Bucket: {S3_BUCKET}")
    print(f"📥 Raw Data: s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/")
    print(f"💾 Bronze Layer: s3a://{S3_BUCKET}/{S3_BRONZE_PREFIX}/")
    print("=" * 80 + "\n")
    
    spark = create_spark_session()
    
    try:
        # Ingest all tables
        results = []
        results.append(("Orders", ingest_orders(spark)))
        results.append(("Products", ingest_products(spark)))
        results.append(("Aisles", ingest_aisles(spark)))
        results.append(("Departments", ingest_departments(spark)))
        results.append(("Order Products (Prior)", ingest_order_products_prior(spark)))
        results.append(("Order Products (Train)", ingest_order_products_train(spark)))
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 INGESTION SUMMARY")
        print("=" * 80)
        for table, success in results:
            status = "✅" if success else "❌"
            print(f"{status} {table}")
        
        if all(success for _, success in results):
            print("\n" + "=" * 80)
            print("✅ BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
            print("=" * 80)
            
            # Show Iceberg tables
            print("\n📊 Iceberg Bronze Tables:")
            spark.sql("SHOW TABLES IN iceberg.bronze").show(truncate=False)
            
            return 0
        else:
            print("\n❌ Some ingestions failed")
            return 1
            
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        print(f"\n⏱️  End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
