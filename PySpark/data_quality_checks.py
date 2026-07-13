"""
Data Quality Checks - Instacart Dataset
Comprehensive data quality validation for Bronze and Silver layers

Quality Checks:
1. Schema validation
2. Null checks
3. Duplicate detection
4. Referential integrity
5. Business rule validation
6. Outlier detection

Author: Data Engineering Team
Date: 2026-07-10
"""

import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, when

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.instacart_config import SPARK_CONFIGS, DATA_QUALITY_RULES
from .utils import (
    validate_schema, check_null_counts, check_duplicate_keys,
    check_referential_integrity, profile_dataframe,
    record_quality_results
)


def create_spark_session():
    """Create Spark session for data quality checks"""
    print("🔧 Creating Spark session for data quality checks...")
    
    builder = SparkSession.builder.appName("Instacart-DataQuality-Checks")
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created\n")
    return spark


class DataQualityResult:
    """Container for data quality check results"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.checks_passed = []
        self.checks_failed = []
        self.warnings = []
    
    def add_pass(self, check_name: str, message: str = ""):
        self.checks_passed.append((check_name, message))
    
    def add_fail(self, check_name: str, message: str):
        self.checks_failed.append((check_name, message))
    
    def add_warning(self, check_name: str, message: str):
        self.warnings.append((check_name, message))
    
    def is_success(self) -> bool:
        return len(self.checks_failed) == 0
    
    def print_summary(self):
        print(f"\n{'=' * 80}")
        print(f"📊 DATA QUALITY REPORT: {self.table_name}")
        print(f"{'=' * 80}")
        
        print(f"\n✅ PASSED ({len(self.checks_passed)} checks):")
        for check_name, message in self.checks_passed:
            print(f"  ✓ {check_name}" + (f": {message}" if message else ""))
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)} checks):")
            for check_name, message in self.warnings:
                print(f"  ⚠  {check_name}: {message}")
        
        if self.checks_failed:
            print(f"\n❌ FAILED ({len(self.checks_failed)} checks):")
            for check_name, message in self.checks_failed:
                print(f"  ✗ {check_name}: {message}")
        
        print(f"\n{'=' * 80}")
        print(f"RESULT: {'✅ PASS' if self.is_success() else '❌ FAIL'}")
        print(f"{'=' * 80}\n")


def check_bronze_orders(spark) -> DataQualityResult:
    """Quality checks for bronze.orders table"""
    result = DataQualityResult("bronze.orders")
    
    try:
        df = spark.table("iceberg.bronze.orders")
        
        # Check 1: Schema validation
        try:
            validate_schema(df, ["order_id", "user_id", "order_number"], "orders")
            result.add_pass("Schema Validation", "All required columns present")
        except ValueError as e:
            result.add_fail("Schema Validation", str(e))
        
        # Check 2: Null checks for critical columns
        null_counts = check_null_counts(df, ["order_id", "user_id"])
        for col_name, null_count in null_counts.items():
            if null_count > 0:
                result.add_fail(f"Null Check ({col_name})", f"{null_count:,} nulls found")
            else:
                result.add_pass(f"Null Check ({col_name})")
        
        # Check 3: Duplicate order_ids
        dup_count, _ = check_duplicate_keys(df, ["order_id"], "orders")
        if dup_count > 0:
            result.add_fail("Duplicate Check", f"{dup_count:,} duplicate order_ids")
        else:
            result.add_pass("Duplicate Check", "No duplicates")
        
        # Check 4: Business rule - order_number > 0
        invalid_order_numbers = df.filter(col("order_number") < 1).count()
        if invalid_order_numbers > 0:
            result.add_fail("Business Rule (order_number)", 
                          f"{invalid_order_numbers:,} orders with invalid order_number")
        else:
            result.add_pass("Business Rule (order_number)")
        
        # Check 5: Valid order_dow (0-6)
        invalid_dow = df.filter((col("order_dow") < 0) | (col("order_dow") > 6)).count()
        if invalid_dow > 0:
            result.add_fail("Business Rule (order_dow)", 
                          f"{invalid_dow:,} orders with invalid day of week")
        else:
            result.add_pass("Business Rule (order_dow)")
        
        # Check 6: Valid order_hour_of_day (0-23)
        invalid_hour = df.filter((col("order_hour_of_day") < 0) | 
                                (col("order_hour_of_day") > 23)).count()
        if invalid_hour > 0:
            result.add_fail("Business Rule (order_hour)", 
                          f"{invalid_hour:,} orders with invalid hour")
        else:
            result.add_pass("Business Rule (order_hour)")
        
    except Exception as e:
        result.add_fail("Table Read", f"Error reading table: {str(e)}")
    
    return result


def check_bronze_products(spark) -> DataQualityResult:
    """Quality checks for bronze.products table"""
    result = DataQualityResult("bronze.products")
    
    try:
        df = spark.table("iceberg.bronze.products")
        
        # Schema validation
        try:
            validate_schema(df, ["product_id", "product_name"], "products")
            result.add_pass("Schema Validation")
        except ValueError as e:
            result.add_fail("Schema Validation", str(e))
        
        # Null checks
        null_product_ids = df.filter(col("product_id").isNull()).count()
        if null_product_ids > 0:
            result.add_fail("Null Check (product_id)", f"{null_product_ids:,} nulls")
        else:
            result.add_pass("Null Check (product_id)")
        
        # Check for missing product names
        null_names = df.filter(col("product_name").isNull() | 
                              (col("product_name") == "")).count()
        if null_names > 0:
            result.add_warning("Null Check (product_name)", 
                             f"{null_names:,} products with missing names")
        else:
            result.add_pass("Null Check (product_name)")
        
        # Duplicate check
        dup_count, _ = check_duplicate_keys(df, ["product_id"], "products")
        if dup_count > 0:
            result.add_fail("Duplicate Check", f"{dup_count:,} duplicates")
        else:
            result.add_pass("Duplicate Check")
    
    except Exception as e:
        result.add_fail("Table Read", f"Error: {str(e)}")
    
    return result


def check_silver_order_products_enriched(spark) -> DataQualityResult:
    """Quality checks for silver.order_products_enriched table"""
    result = DataQualityResult("silver.order_products_enriched")
    
    try:
        df = spark.table("iceberg.silver.order_products_enriched")
        df_orders = spark.table("iceberg.bronze.orders")
        df_products = spark.table("iceberg.bronze.products")
        
        # Schema validation
        required_cols = ["order_id", "product_id", "product_name", 
                        "aisle", "department"]
        try:
            validate_schema(df, required_cols, "order_products_enriched")
            result.add_pass("Schema Validation")
        except ValueError as e:
            result.add_fail("Schema Validation", str(e))
        
        # Duplicate check
        dup_count, _ = check_duplicate_keys(df, ["order_id", "product_id"],
                                           "order_products_enriched")
        if dup_count > 0:
            result.add_fail("Duplicate Check", f"{dup_count:,} duplicates")
        else:
            result.add_pass("Duplicate Check")
        
        # Referential integrity - order_id
        orphaned_orders, _ = check_referential_integrity(
            df, df_orders, "order_id", "order_id", "order_products -> orders"
        )
        if orphaned_orders > 0:
            result.add_fail("FK Integrity (order_id)", 
                          f"{orphaned_orders:,} orphaned orders")
        else:
            result.add_pass("FK Integrity (order_id)")
        
        # Referential integrity - product_id
        orphaned_products, _ = check_referential_integrity(
            df, df_products, "product_id", "product_id", 
            "order_products -> products"
        )
        if orphaned_products > 0:
            result.add_warning("FK Integrity (product_id)",
                             f"{orphaned_products:,} orphaned products (should be filtered)")
        else:
            result.add_pass("FK Integrity (product_id)")
        
        # Check enrichment completeness
        null_product_names = df.filter(col("product_name").isNull()).count()
        if null_product_names > 0:
            result.add_fail("Enrichment Quality", 
                          f"{null_product_names:,} missing product names")
        else:
            result.add_pass("Enrichment Quality")
        
    except Exception as e:
        result.add_fail("Table Read", f"Error: {str(e)}")
    
    return result


def check_silver_user_order_summary(spark) -> DataQualityResult:
    """Quality checks for silver.user_order_summary table"""
    result = DataQualityResult("silver.user_order_summary")
    
    try:
        df = spark.table("iceberg.silver.user_order_summary")
        
        # Schema validation
        required_cols = ["user_id", "total_orders", "avg_basket_size", 
                        "reorder_rate", "user_segment"]
        try:
            validate_schema(df, required_cols, "user_order_summary")
            result.add_pass("Schema Validation")
        except ValueError as e:
            result.add_fail("Schema Validation", str(e))
        
        # Business rule - total_orders > 0
        invalid_orders = df.filter(col("total_orders") < 1).count()
        if invalid_orders > 0:
            result.add_fail("Business Rule (total_orders)", 
                          f"{invalid_orders:,} users with 0 orders")
        else:
            result.add_pass("Business Rule (total_orders)")
        
        # Business rule - reorder_rate between 0 and 1
        invalid_reorder = df.filter(
            (col("reorder_rate") < 0) | (col("reorder_rate") > 1)
        ).count()
        if invalid_reorder > 0:
            result.add_fail("Business Rule (reorder_rate)", 
                          f"{invalid_reorder:,} invalid reorder rates")
        else:
            result.add_pass("Business Rule (reorder_rate)")
        
        # Check user segmentation
        null_segments = df.filter(col("user_segment").isNull()).count()
        if null_segments > 0:
            result.add_fail("Segmentation", f"{null_segments:,} users without segment")
        else:
            result.add_pass("Segmentation")
    
    except Exception as e:
        result.add_fail("Table Read", f"Error: {str(e)}")
    
    return result


def main():
    """Main execution function"""
    
    print("\n" + "=" * 80)
    print("🔍 INSTACART DATA QUALITY CHECKS")
    print("=" * 80)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")
    
    spark = create_spark_session()
    
    try:
        # Run all quality checks
        results = []
        
        print("🔍 Checking Bronze Layer...")
        results.append(check_bronze_orders(spark))
        results.append(check_bronze_products(spark))
        
        print("\n🔍 Checking Silver Layer...")
        results.append(check_silver_order_products_enriched(spark))
        results.append(check_silver_user_order_summary(spark))
        
        # Print all results
        for result in results:
            result.print_summary()
        
        # Record quality results to MongoDB (quality ledger)
        run_id = record_quality_results(results)
        print(f"\n📝 Quality results recorded to MongoDB (run_id: {run_id})")
        
        # Overall summary
        total_checks = sum(len(r.checks_passed) + len(r.checks_failed) 
                          for r in results)
        total_passed = sum(len(r.checks_passed) for r in results)
        total_failed = sum(len(r.checks_failed) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)
        
        all_passed = all(r.is_success() for r in results)
        
        print("\n" + "=" * 80)
        print("🎯 OVERALL DATA QUALITY SUMMARY")
        print("=" * 80)
        print(f"Total Checks:   {total_checks}")
        print(f"✅ Passed:      {total_passed}")
        print(f"❌ Failed:      {total_failed}")
        print(f"⚠️  Warnings:    {total_warnings}")
        print(f"\nFinal Result:   {'✅ ALL CHECKS PASSED' if all_passed else '❌ SOME CHECKS FAILED'}")
        print("=" * 80)
        
        return 0 if all_passed else 1
        
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
