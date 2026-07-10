"""
Silver Layer Transformation - Instacart Dataset
Cleans, joins, deduplicates, and enriches data from Bronze to Silver layer

Transformations:
1. UNION order_products_prior + order_products_train
2. Join with products, aisles, departments (create hierarchy)
3. Deduplicate by (order_id, product_id)
4. Validate referential integrity
5. Create orders_enriched with user metrics
6. Create products_hierarchy with flattened structure
7. Create user_order_summary aggregates

Author: Data Engineering Team
Date: 2026-07-10
"""

import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, min as spark_min,
    datediff, when, lit, current_timestamp, concat_ws,
    sum as spark_sum, countDistinct, row_number
)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.instacart_config import (
    S3_BUCKET, S3_BRONZE_PREFIX, S3_SILVER_PREFIX,
    SPARK_CONFIGS
)


def create_spark_session():
    """Create Spark session with Iceberg and S3 support"""
    print("🔧 Creating Spark session...")
    
    builder = SparkSession.builder.appName("Instacart-Silver-Transformation")
    
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    return spark


def create_orders_enriched(spark):
    """
    Create silver.orders_enriched table
    
    Enrichments:
    - Add user-level metrics (first_order, last_order)
    - Add order recency
    - Flag first orders
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating orders_enriched")
    print("=" * 80)
    
    try:
        # Read Bronze orders
        df_orders = spark.table("iceberg.bronze.orders")
        
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
        
        # Write to Silver
        iceberg_table = "iceberg.silver.orders_enriched"
        print(f"💾 Writing to: {iceberg_table}")
        
        df_enriched.writeTo(iceberg_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_enriched.count():,} orders")
        
        # Show sample
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
    3. Deduplicate
    4. Validate FK integrity
    5. Partition by department_id for query performance
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating order_products_enriched")
    print("=" * 80)
    
    try:
        # Step 1: UNION prior + train
        print("📊 Step 1: UNION order_products_prior + train")
        df_prior = spark.table("iceberg.bronze.order_products_prior")
        df_train = spark.table("iceberg.bronze.order_products_train")
        
        df_order_products = df_prior.unionAll(df_train)
        total_count = df_order_products.count()
        print(f"✅ Total order-product records: {total_count:,}")
        
        # Step 2: Join with products, aisles, departments
        print("\n📊 Step 2: Join with product hierarchy")
        df_products = spark.table("iceberg.bronze.products")
        df_aisles = spark.table("iceberg.bronze.aisles")
        df_departments = spark.table("iceberg.bronze.departments")
        
        # Cache small tables for broadcast join
        df_products.cache()
        df_aisles.cache()
        df_departments.cache()
        
        df_enriched = df_order_products \
            .join(df_products, "product_id", "left") \
            .join(df_aisles, "aisle_id", "left") \
            .join(df_departments, "department_id", "left")
        
        # Step 3: Deduplication
        print("\n📊 Step 3: Deduplication")
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
        
        # Check for orphaned product_ids
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
        iceberg_table = "iceberg.silver.order_products_enriched"
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
        df_products = spark.table("iceberg.bronze.products")
        df_aisles = spark.table("iceberg.bronze.aisles")
        df_departments = spark.table("iceberg.bronze.departments")
        
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
                       col("product_name"))  # Can add cleaning logic here
        
        iceberg_table = "iceberg.silver.products_hierarchy"
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


def create_user_order_summary(spark):
    """
    Create silver.user_order_summary table
    
    User-level aggregates:
    - total_orders
    - avg_basket_size
    - reorder_rate
    - first/last order dates
    """
    print("\n" + "=" * 80)
    print("🔄 SILVER LAYER: Creating user_order_summary")
    print("=" * 80)
    
    try:
        df_orders = spark.table("iceberg.silver.orders_enriched")
        df_order_products = spark.table("iceberg.silver.order_products_enriched")
        
        # Calculate basket sizes
        df_basket = df_order_products \
            .groupBy("order_id") \
            .agg(
                count("*").alias("basket_size"),
                spark_sum(when(col("reordered") == 1, 1).otherwise(0)).alias("reordered_items")
            )
        
        # Join with orders and aggregate by user
        df_user_summary = df_orders \
            .join(df_basket, "order_id", "left") \
            .groupBy("user_id") \
            .agg(
                countDistinct("order_id").alias("total_orders"),
                avg("basket_size").alias("avg_basket_size"),
                (spark_sum("reordered_items") / spark_sum("basket_size")).alias("reorder_rate"),
                spark_max("order_number").alias("max_order_number")
            )
        
        # Add user segmentation
        df_user_summary = df_user_summary \
            .withColumn("user_segment",
                       when(col("total_orders") == 1, "New")
                       .when(col("total_orders") <= 5, "Active")
                       .when(col("total_orders") > 5, "Power")
                       .otherwise("Unknown"))
        
        iceberg_table = "iceberg.silver.user_order_summary"
        df_user_summary.writeTo(iceberg_table) \
            .using("iceberg") \
            .createOrReplace()
        
        print(f"✅ Successfully wrote {df_user_summary.count():,} users")
        
        # Show segmentation
        print("\n📊 User segmentation:")
        df_user_summary.groupBy("user_segment") \
            .agg(
                count("*").alias("user_count"),
                avg("total_orders").alias("avg_orders"),
                avg("reorder_rate").alias("avg_reorder_rate")
            ) \
            .show()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise


def main():
    """Main execution function"""
    
    print("\n" + "=" * 80)
    print("🚀 INSTACART SILVER LAYER TRANSFORMATION")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    spark = create_spark_session()
    
    try:
        # Execute transformations in order
        results = []
        results.append(("orders_enriched", create_orders_enriched(spark)))
        results.append(("order_products_enriched", create_order_products_enriched(spark)))
        results.append(("products_hierarchy", create_products_hierarchy(spark)))
        results.append(("user_order_summary", create_user_order_summary(spark)))
        
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
            print("\n📊 Silver Tables:")
            spark.sql("SHOW TABLES IN iceberg.silver").show(truncate=False)
            
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
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
