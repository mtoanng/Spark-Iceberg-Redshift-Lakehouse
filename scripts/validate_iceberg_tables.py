"""
Validate Iceberg tables exist and have expected row counts

Usage:
    python scripts/validate_iceberg_tables.py --layer bronze
    python scripts/validate_iceberg_tables.py --layer silver
"""

import sys
import argparse
from pathlib import Path
from pyspark.sql import SparkSession

# Add project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.instacart_config import SPARK_CONFIGS, ICEBERG_TABLES


def validate_tables(spark, layer):
    """Validate tables exist and have data"""
    
    print(f"\n{'=' * 80}")
    print(f"🔍 VALIDATING {layer.upper()} LAYER TABLES")
    print(f"{'=' * 80}\n")
    
    tables = ICEBERG_TABLES.get(layer, {})
    if not tables:
        print(f"❌ No tables defined for layer: {layer}")
        return False
    
    all_valid = True
    
    for table_name, table_path in tables.items():
        try:
            df = spark.table(table_path)
            count = df.count()
            
            if count > 0:
                print(f"✅ {table_name}: {count:,} rows")
            else:
                print(f"⚠️  {table_name}: 0 rows (empty table)")
                all_valid = False
                
        except Exception as e:
            print(f"❌ {table_name}: Table not found or error - {str(e)}")
            all_valid = False
    
    print(f"\n{'=' * 80}")
    if all_valid:
        print(f"✅ ALL {layer.upper()} TABLES VALIDATED")
    else:
        print(f"❌ SOME {layer.upper()} TABLES INVALID")
    print(f"{'=' * 80}\n")
    
    return all_valid


def main():
    parser = argparse.ArgumentParser(description='Validate Iceberg tables')
    parser.add_argument('--layer', required=True, choices=['bronze', 'silver'],
                       help='Layer to validate')
    args = parser.parse_args()
    
    # Create Spark session
    builder = SparkSession.builder.appName("Validate-Iceberg-Tables")
    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        success = validate_tables(spark, args.layer)
        return 0 if success else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
