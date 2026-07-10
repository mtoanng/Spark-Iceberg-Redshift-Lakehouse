"""
PySpark Utility Functions
Reusable functions for Spark data processing

Author: Data Engineering Team
Date: 2026-07-10
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, count, sum as spark_sum, when
from pyspark.sql.types import StructType
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


def validate_schema(df: DataFrame, expected_columns: List[str], table_name: str) -> bool:
    """
    Validate that DataFrame contains expected columns
    
    Args:
        df: Spark DataFrame
        expected_columns: List of required column names
        table_name: Name of table for logging
        
    Returns:
        bool: True if validation passes, raises Exception otherwise
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing_columns = expected_set - actual_columns
    
    if missing_columns:
        raise ValueError(
            f"Schema validation failed for {table_name}. "
            f"Missing columns: {missing_columns}"
        )
    
    logger.info(f"✅ Schema validation passed for {table_name}")
    return True


def check_null_counts(df: DataFrame, columns: List[str] = None) -> Dict[str, int]:
    """
    Check null counts for specified columns
    
    Args:
        df: Spark DataFrame
        columns: List of columns to check (None = all columns)
        
    Returns:
        Dict[str, int]: Column name -> null count
    """
    if columns is None:
        columns = df.columns
    
    null_counts = {}
    
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count
        
        if null_count > 0:
            logger.warning(f"⚠️  Column '{column}' has {null_count:,} null values")
    
    return null_counts


def check_duplicate_keys(df: DataFrame, key_columns: List[str], table_name: str) -> Tuple[int, DataFrame]:
    """
    Check for duplicate records based on key columns
    
    Args:
        df: Spark DataFrame
        key_columns: List of columns that form the primary key
        table_name: Name of table for logging
        
    Returns:
        Tuple[int, DataFrame]: (duplicate_count, duplicates_df)
    """
    total_count = df.count()
    distinct_count = df.select(key_columns).distinct().count()
    duplicate_count = total_count - distinct_count
    
    if duplicate_count > 0:
        logger.warning(
            f"⚠️  Found {duplicate_count:,} duplicates in {table_name} "
            f"({duplicate_count/total_count*100:.2f}%)"
        )
        
        # Find duplicate records
        duplicates_df = df.groupBy(key_columns) \
            .count() \
            .filter(col("count") > 1) \
            .orderBy(col("count").desc())
        
        return duplicate_count, duplicates_df
    else:
        logger.info(f"✅ No duplicates found in {table_name}")
        return 0, None


def deduplicate_dataframe(df: DataFrame, key_columns: List[str], 
                          order_by: str = None) -> DataFrame:
    """
    Deduplicate DataFrame by key columns
    
    Args:
        df: Spark DataFrame
        key_columns: Columns to use for deduplication
        order_by: Column to order by when choosing which record to keep
        
    Returns:
        DataFrame: Deduplicated DataFrame
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    if order_by:
        window = Window.partitionBy(key_columns).orderBy(col(order_by).desc())
    else:
        window = Window.partitionBy(key_columns).orderBy(lit(1))
    
    df_deduped = df \
        .withColumn("_row_num", row_number().over(window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    return df_deduped


def check_referential_integrity(
    df_child: DataFrame, 
    df_parent: DataFrame,
    child_fk: str,
    parent_pk: str,
    relationship_name: str
) -> Tuple[int, DataFrame]:
    """
    Check referential integrity between two DataFrames
    
    Args:
        df_child: Child DataFrame with foreign key
        df_parent: Parent DataFrame with primary key
        child_fk: Foreign key column name in child
        parent_pk: Primary key column name in parent
        relationship_name: Description of relationship for logging
        
    Returns:
        Tuple[int, DataFrame]: (orphaned_count, orphaned_records_df)
    """
    # Find orphaned records
    orphaned_df = df_child \
        .select(child_fk).distinct() \
        .join(df_parent.select(parent_pk), 
              col(child_fk) == col(parent_pk), 
              "left_anti")
    
    orphaned_count = orphaned_df.count()
    
    if orphaned_count > 0:
        total_distinct = df_child.select(child_fk).distinct().count()
        logger.warning(
            f"⚠️  Referential integrity violation in {relationship_name}: "
            f"{orphaned_count:,} orphaned keys "
            f"({orphaned_count/total_distinct*100:.2f}%)"
        )
        return orphaned_count, orphaned_df
    else:
        logger.info(f"✅ Referential integrity valid for {relationship_name}")
        return 0, None


def profile_dataframe(df: DataFrame, table_name: str, sample_size: int = 5):
    """
    Print comprehensive profile of DataFrame
    
    Args:
        df: Spark DataFrame
        table_name: Name of table for display
        sample_size: Number of sample rows to show
    """
    print("\n" + "=" * 80)
    print(f"📊 DATAFRAME PROFILE: {table_name}")
    print("=" * 80)
    
    # Row count
    row_count = df.count()
    print(f"📏 Row Count: {row_count:,}")
    
    # Schema
    print(f"\n📋 Schema:")
    df.printSchema()
    
    # Null counts
    print(f"\n🔍 Null Counts:")
    null_counts = check_null_counts(df)
    for col_name, null_count in sorted(null_counts.items()):
        if null_count > 0:
            pct = null_count / row_count * 100
            print(f"  {col_name}: {null_count:,} ({pct:.2f}%)")
    
    # Sample data
    print(f"\n📄 Sample Data (first {sample_size} rows):")
    df.show(sample_size, truncate=False)
    
    print("=" * 80)


def add_audit_columns(df: DataFrame, source_system: str = "instacart") -> DataFrame:
    """
    Add standard audit columns to DataFrame
    
    Args:
        df: Spark DataFrame
        source_system: Source system identifier
        
    Returns:
        DataFrame: DataFrame with audit columns added
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    return df \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source_system", lit(source_system))


def calculate_data_freshness(df: DataFrame, timestamp_col: str) -> Dict[str, any]:
    """
    Calculate data freshness metrics
    
    Args:
        df: Spark DataFrame
        timestamp_col: Name of timestamp column
        
    Returns:
        Dict with min_date, max_date, record_count
    """
    from pyspark.sql.functions import min as spark_min, max as spark_max, count
    
    stats = df.agg(
        spark_min(timestamp_col).alias("min_date"),
        spark_max(timestamp_col).alias("max_date"),
        count("*").alias("record_count")
    ).collect()[0]
    
    return {
        "min_date": stats["min_date"],
        "max_date": stats["max_date"],
        "record_count": stats["record_count"]
    }


def compute_basic_stats(df: DataFrame, numeric_columns: List[str]) -> DataFrame:
    """
    Compute basic statistics for numeric columns
    
    Args:
        df: Spark DataFrame
        numeric_columns: List of numeric column names
        
    Returns:
        DataFrame: Summary statistics
    """
    from pyspark.sql.functions import avg, min as spark_min, max as spark_max, stddev, count
    
    agg_exprs = []
    for col_name in numeric_columns:
        agg_exprs.extend([
            count(col_name).alias(f"{col_name}_count"),
            avg(col_name).alias(f"{col_name}_avg"),
            spark_min(col_name).alias(f"{col_name}_min"),
            spark_max(col_name).alias(f"{col_name}_max"),
            stddev(col_name).alias(f"{col_name}_stddev")
        ])
    
    return df.agg(*agg_exprs)


def filter_outliers(df: DataFrame, column: str, std_devs: float = 3.0) -> DataFrame:
    """
    Filter outliers using standard deviation method
    
    Args:
        df: Spark DataFrame
        column: Column name to check for outliers
        std_devs: Number of standard deviations for threshold
        
    Returns:
        DataFrame: Filtered DataFrame with outliers removed
    """
    from pyspark.sql.functions import avg, stddev
    
    stats = df.agg(
        avg(column).alias("mean"),
        stddev(column).alias("stddev")
    ).collect()[0]
    
    mean = stats["mean"]
    std = stats["stddev"]
    
    lower_bound = mean - (std_devs * std)
    upper_bound = mean + (std_devs * std)
    
    filtered_df = df.filter(
        (col(column) >= lower_bound) & (col(column) <= upper_bound)
    )
    
    removed_count = df.count() - filtered_df.count()
    logger.info(f"🗑️  Removed {removed_count:,} outliers from {column}")
    
    return filtered_df


def write_to_iceberg(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: List[str] = None
):
    """
    Write DataFrame to Iceberg table with standard properties
    
    Args:
        df: Spark DataFrame
        table_name: Fully qualified Iceberg table name (e.g., 'iceberg.bronze.orders')
        mode: Write mode ('overwrite', 'append')
        partition_by: List of partition columns
    """
    writer = df.writeTo(table_name).using("iceberg")
    
    # Standard table properties
    writer = writer \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy")
    
    # Add partitioning if specified
    if partition_by:
        writer = writer.partitionedBy(*partition_by)
    
    # Execute write
    if mode == "overwrite":
        writer.createOrReplace()
    elif mode == "append":
        writer.append()
    else:
        raise ValueError(f"Invalid mode: {mode}")
    
    logger.info(f"✅ Successfully wrote to {table_name}")


def log_transformation_metrics(
    table_name: str,
    input_count: int,
    output_count: int,
    transformation_type: str
):
    """
    Log transformation metrics for monitoring
    
    Args:
        table_name: Name of table being transformed
        input_count: Number of input records
        output_count: Number of output records
        transformation_type: Description of transformation
    """
    records_delta = output_count - input_count
    pct_change = (records_delta / input_count * 100) if input_count > 0 else 0
    
    print("\n" + "=" * 80)
    print(f"📊 TRANSFORMATION METRICS: {table_name}")
    print("=" * 80)
    print(f"Transformation: {transformation_type}")
    print(f"Input Records:  {input_count:,}")
    print(f"Output Records: {output_count:,}")
    print(f"Delta:          {records_delta:+,} ({pct_change:+.2f}%)")
    print("=" * 80)


if __name__ == "__main__":
    # Test utilities
    print("✅ PySpark utilities loaded successfully")
    print("Available functions:")
    print("  - validate_schema")
    print("  - check_null_counts")
    print("  - check_duplicate_keys")
    print("  - deduplicate_dataframe")
    print("  - check_referential_integrity")
    print("  - profile_dataframe")
    print("  - add_audit_columns")
    print("  - calculate_data_freshness")
    print("  - compute_basic_stats")
    print("  - filter_outliers")
    print("  - write_to_iceberg")
    print("  - log_transformation_metrics")
