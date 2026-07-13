"""
Silver Layer Transformation - AWS Glue Job (Adapted from Databricks version)
Cleans, joins, deduplicates, and enriches data from Bronze to Silver layer

CHANGES FROM ORIGINAL:
- Added AWS Glue imports
- Changed catalog from 'iceberg.' to 'glue_catalog.'
- Removed config file import
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
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, min as spark_min,
    when, lit, current_timestamp,
    sum as spark_sum, countDistinct, row_number
)

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])


def create_glue_context():
    """Create Glue context with Iceberg configuration"""
    print("🔧 Creating Glue context...")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    # Configure Iceberg with Glue Catalog
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Glue context created")
    return glueContext, spark


def create_orders_enriched(spark):
    """
    Create silver.orders_enriched table
    
    Enrichments:
    - Add user-level metrics (first_order, last_order)
    - Flag first orders
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating orders_enriched")
    print("=" * 80)
    
    try:
        # CHANGED: iceberg.bronze → glue_catalog.bronze
        df_orders = spark.table("glue_catalog.bronze.orders")
        
        print(f"📊 Bronze orders count: {df_orders.count():,}")
        
        # Calculate user-level metrics
        user_window = Window.partitionBy("user_id")
        
        df_enriched = df_orders \
            .withColumn("user_total_orders", count("*").over(user_window)) \
            .withColumn("user_first_order_number", spark_min("order_number").over(user_window)) \
            .withColumn("user_last_order_number", spark_max("order_number").over(user_window)) \
            .withColumn("is_first_order", 
                       when(col("order_number") == col("user_first_order_number"), True)
                       .otherwise(False))
        
        # CHANGED: Write to glue_catalog.silver
        iceberg_table = "glue_catalog.silver.orders_enriched"
        print(f"💾 Writing to: {iceberg_table}")
        
        df_enriched.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_enriched.count():,} orders")
        
        print("\n📋 Sample:")
        df_enriched.select("order_id", "user_id", "order_number", "is_first_order", 
                          "user_total_orders").show(5)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def create_order_products_enriched(spark):
    """
    Create silver.order_products_enriched table
    
    Steps:
    1. UNION prior + train datasets
    2. Join with products, aisles, departments
    3. Deduplicate by (order_id, product_id)
    4. Validate FK integrity
    5. Partition by department_id
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating order_products_enriched")
    print("=" * 80)
    
    try:
        # Step 1: UNION prior + train
        print("📊 Step 1: UNION order_products_prior + train")
        df_prior = spark.table("glue_catalog.bronze.order_products_prior")
        df_train = spark.table("glue_catalog.bronze.order_products_train")
        
        df_order_products = df_prior.unionAll(df_train)
        total_count = df_order_products.count()
        print(f"✅ Total order-product records: {total_count:,}")
        
        # Step 2: Join with products, aisles, departments
        print("\n📊 Step 2: Join with product hierarchy")
        df_products = spark.table("glue_catalog.bronze.products")
        df_aisles = spark.table("glue_catalog.bronze.aisles")
        df_departments = spark.table("glue_catalog.bronze.departments")
        
        # Cache small tables for broadcast join
        df_products.cache()
        df_aisles.cache()
        df_departments.cache()
        
        df_enriched = df_order_products \
            .join(df_products, "product_id", "left") \
            .join(df_aisles, "aisle_id", "left") \
            .join(df_departments, "department_id", "left")
        
        # Step 3: Deduplication
        print("\n📊 Step 3: Deduplication by (order_id, product_id)")
        before_dedup = df_enriched.count()
        
        window = Window.partitionBy("order_id", "product_id").orderBy(col("add_to_cart_order"))
        df_enriched = df_enriched \
            .withColumn("row_num", row_number().over(window)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        after_dedup = df_enriched.count()
        duplicates = before_dedup - after_dedup
        
        if duplicates > 0:
            print(f"⚠️  Removed {duplicates:,} duplicate records ({duplicates/before_dedup*100:.2f}%)")
        else:
            print(f"✅ No duplicates found")
        
        # Step 4: Validate FK integrity
        print("\n📊 Step 4: Validate referential integrity")
        
        orphaned_products = df_enriched.filter(col("product_name").isNull()).count()
        
        if orphaned_products > 0:
            print(f"⚠️  Found {orphaned_products:,} orphaned product_ids - filtering out")
            df_enriched = df_enriched.filter(col("product_name").isNotNull())
        else:
            print(f"✅ All product_ids have valid references")
        
        # Step 5: Add metadata
        df_enriched = df_enriched \
            .withColumn("silver_timestamp", current_timestamp()) \
            .select(
                "order_id",
                "product_id",
                "add_to_cart_order",
                "reordered",
                "eval_set",
                "product_name",
                "aisle_id",
                "aisle",
                "department_id",
                "department",
                "silver_timestamp"
            )
        
        # Step 6: Write to Silver with partitioning
        iceberg_table = "glue_catalog.silver.order_products_enriched"
        print(f"\n💾 Writing to: {iceberg_table}")
        print(f"📂 Partitioning by: department_id")
        
        df_enriched.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .partitionedBy("department_id") \
            .createOrReplace()
        
        final_count = df_enriched.count()
        print(f"✅ Successfully wrote {final_count:,} records")
        
        # Show stats
        print("\n📊 Department distribution:")
        df_enriched.groupBy("department") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc()) \
            .show(10)
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def create_products_hierarchy(spark):
    """
    Create silver.products_hierarchy table
    
    Flattened product hierarchy: department → aisle → product
    Add derived attributes (is_organic, etc.)
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating products_hierarchy")
    print("=" * 80)
    
    try:
        df_products = spark.table("glue_catalog.bronze.products")
        df_aisles = spark.table("glue_catalog.bronze.aisles")
        df_departments = spark.table("glue_catalog.bronze.departments")
        
        df_hierarchy = df_products \
            .join(df_aisles, "aisle_id", "left") \
            .join(df_departments, "department_id", "left")
        
        # Add derived attributes
        df_hierarchy = df_hierarchy \
            .withColumn("is_organic", 
                       col("product_name").contains("Organic")) \
            .withColumn("is_gluten_free",
                       col("product_name").contains("Gluten Free")) \
            .withColumn("product_name_clean",
                       col("product_name"))
        
        iceberg_table = "glue_catalog.silver.products_hierarchy"
        df_hierarchy.writeTo(iceberg_table) \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_hierarchy.count():,} products")
        
        # Show organic stats
        organic_count = df_hierarchy.filter(col("is_organic")).count()
        total_count = df_hierarchy.count()
        print(f"\n📊 Organic products: {organic_count:,} ({organic_count/total_count*100:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def main():
    """Main execution function for AWS Glue Job"""
    
    print("\n" + "=" * 80)
    print("🚀 INSTACART SILVER LAYER TRANSFORMATION (AWS Glue)")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    # Create Glue context
    glueContext, spark = create_glue_context()
    
    # Initialize Glue Job
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    
    try:
        # Execute transformations in order
        results = []
        results.append(("orders_enriched", create_orders_enriched(spark)))
        results.append(("order_products_enriched", create_order_products_enriched(spark)))
        results.append(("products_hierarchy", create_products_hierarchy(spark)))
        
        # Summary
        print("\n" + "=" * 80)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 80)
        for table, success in results:
            status = "✅" if success else "❌"
            print(f"{status} {table}")
        
        if all(success for _, success in results):
            print("\n" + "=" * 80)
            print("✅ SILVER LAYER TRANSFORMATION COMPLETED")
            print("=" * 80)
            
            # Show Silver tables
            print("\n📊 Glue Catalog Silver Tables:")
            spark.sql("SHOW TABLES IN glue_catalog.silver").show(truncate=False)
            
            # Commit Glue Job
            job.commit()
            return 0
        else:
            print("\n❌ Some transformations failed")
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
