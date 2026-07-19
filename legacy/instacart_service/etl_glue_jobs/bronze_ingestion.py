"""
Bronze Layer Ingestion - AWS Glue Job (Adapted from Databricks version)
Reads raw CSV files from S3 and writes to Iceberg Bronze tables

CHANGES FROM ORIGINAL:
- Added AWS Glue imports (GlueContext, Job, getResolvedOptions)
- Changed Spark session creation to use GlueContext
- Changed catalog from 'iceberg.' to 'glue_catalog.'
- Removed config file import (use job parameters instead)
- Added job.init() and job.commit()

Author: Data Engineering Team
Date: 2026-07-13 (Refactored for AWS Glue)
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, current_timestamp

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "S3_RAW_PREFIX"])

# Instacart file mappings (hardcoded since we removed config import)
INSTACART_FILES = {
    "orders": "orders.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv"
}


def create_glue_context():
    """Create Glue context and configure Iceberg"""
    print("🔧 Creating Glue context...")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Configure Iceberg with Glue Catalog
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['S3_BUCKET']}/warehouse/")
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Glue context created successfully")
    return glueContext, spark


def validate_csv_schema(df, table_name):
    """Validate CSV and show stats"""
    actual_count = df.count()
    print(f"✅ Loaded {actual_count:,} rows for {table_name}")
    return True


def ingest_orders(spark):
    """Ingest orders.csv to Bronze layer"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Orders")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['orders']}"
    print(f"📂 Reading from: {raw_path}")
    
    try:
        df_orders = spark.read.csv(raw_path, header=True, inferSchema=True)
        validate_csv_schema(df_orders, "orders")
        
        df_orders = df_orders \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["orders"]))
        
        print(f"📊 Schema:")
        df_orders.printSchema()
        
        # CHANGED: iceberg.bronze → glue_catalog.bronze
        iceberg_table = "glue_catalog.bronze.orders"
        print(f"💾 Writing to Glue Catalog table: {iceberg_table}")
        
        df_orders.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_orders.count():,} records")
        df_orders.select("order_id", "user_id", "order_number", "order_dow").show(5)
        
        return True
        
    except Exception as e:
        print(f"❌ Error ingesting orders: {str(e)}")
        raise


def ingest_order_products_prior(spark):
    """Ingest order_products__prior.csv (LARGEST file: 32.4M rows)"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Order Products (Prior)")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['order_products_prior']}"
    print(f"📂 Reading from: {raw_path}")
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        validate_csv_schema(df, "order_products_prior")
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["order_products_prior"])) \
            .withColumn("eval_set", lit("prior"))
        
        df.writeTo("glue_catalog.bronze.order_products_prior") \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def ingest_order_products_train(spark):
    """Ingest order_products__train.csv"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Order Products (Train)")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['order_products_train']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        validate_csv_schema(df, "order_products_train")
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["order_products_train"])) \
            .withColumn("eval_set", lit("train"))
        
        df.writeTo("glue_catalog.bronze.order_products_train") \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def ingest_products(spark):
    """Ingest products.csv"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Products")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['products']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        validate_csv_schema(df, "products")
        
        df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(INSTACART_FILES["products"]))
        
        df.writeTo("glue_catalog.bronze.products") \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} records")
        df.select("product_id", "product_name").show(10, truncate=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def ingest_aisles(spark):
    """Ingest aisles.csv"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Aisles")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['aisles']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        df.writeTo("glue_catalog.bronze.aisles") \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} aisles")
        return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def ingest_departments(spark):
    """Ingest departments.csv"""
    print("\n" + "=" * 80)
    print("📥 BRONZE LAYER: Ingesting Departments")
    print("=" * 80)
    
    raw_path = f"s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/{INSTACART_FILES['departments']}"
    
    try:
        df = spark.read.csv(raw_path, header=True, inferSchema=True)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        df.writeTo("glue_catalog.bronze.departments") \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df.count():,} departments")
        return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def main():
    """Main execution function for AWS Glue Job"""
    
    print("\n" + "=" * 80)
    print("🚀 INSTACART BRONZE LAYER INGESTION (AWS Glue)")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🪣 S3 Bucket: {args['S3_BUCKET']}")
    print(f"📥 Raw Data: s3://{args['S3_BUCKET']}/{args['S3_RAW_PREFIX']}/")
    print("=" * 80 + "\n")
    
    # Create Glue context (replaces SparkSession for Glue)
    glueContext, spark = create_glue_context()
    
    # Initialize Glue Job
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    
    try:
        # Ingest all tables (same logic as original)
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
            
            # Show Glue Catalog tables
            print("\n📊 Glue Catalog Bronze Tables:")
            spark.sql("SHOW TABLES IN glue_catalog.bronze").show(truncate=False)
            
            # Commit Glue Job (important!)
            job.commit()
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


if __name__ == "__main__":
    sys.exit(main())
