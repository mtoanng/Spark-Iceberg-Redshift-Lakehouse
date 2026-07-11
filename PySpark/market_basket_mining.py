"""
Market Basket Mining — Spark MLlib FPGrowth (OPTIONAL / BONUS)

This job is NOT a blocking dependency for the pipeline. The core Definition
of Done requires only dbt + Iceberg + DuckDB service to run. This job adds the
market basket rules differentiator: "which products are frequently bought
together?"

Runs on Spark OSS (local dev or EC2) after the Silver layer is built. Writes association
rules to ``iceberg.gold.market_basket_rules``.

minSupport / minConfidence rationale:
  - The Instacart dataset has ~3.4M orders and ~50K unique products.
  - minSupport=0.001 means a rule must appear in at least ~3,400 orders to be
    considered frequent. This filters out noise while keeping meaningful
    co-purchase patterns.
  - minConfidence=0.05 (5%) is intentionally low because market basket data
    is sparse — even strong associations rarely exceed 10-15% confidence.
    A higher threshold would miss most valid rules.

Usage:
    spark-submit pyspark/market_basket_mining.py
    (or run as a Spark job after silver_transformation.py)

Author: Data Engineering Team
Date: 2026-07-11
"""

import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, size, explode
from pyspark.ml.fpm import FPGrowth

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.instacart_config import SPARK_CONFIGS, S3_BUCKET


def create_spark_session():
    """Create Spark session with Iceberg and S3 support."""
    print("Creating Spark session for FPGrowth...")

    builder = SparkSession.builder.appName("Instacart-Market-Basket-Mining")

    for key, value in SPARK_CONFIGS.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_fpgrowth(spark):
    """
    Run FPGrowth market basket mining on order-product data.

    Steps:
      1. Read silver.order_products_enriched
      2. Group by order_id, collect product_id lists (baskets)
      3. Fit FPGrowth model
      4. Extract association rules (antecedent, consequent, confidence, lift)
      5. Write to gold.market_basket_rules as Iceberg table
    """
    print("\n" + "=" * 80)
    print("Market Basket Mining — FPGrowth")
    print("=" * 80)

    # --- 1. Read silver order_products ---
    df_order_products = spark.table("iceberg.silver.order_products_enriched")
    print(f"Total order-product records: {df_order_products.count():,}")

    # --- 2. Build baskets: 1 row per order, items = list of product_ids ---
    baskets = df_order_products.groupBy("order_id").agg(
        collect_list("product_id").alias("items")
    )

    # Filter out very small baskets (1 item = no associations possible)
    baskets = baskets.filter(size(col("items")) >= 2)
    basket_count = baskets.count()
    print(f"Eligible baskets (>= 2 items): {basket_count:,}")

    # --- 3. Fit FPGrowth ---
    # minSupport: fraction of all transactions that must contain the itemset.
    # With ~3.4M orders, 0.001 = ~3,400 orders minimum.
    # minConfidence: 5% — sparse data, keep threshold low.
    print("\nFitting FPGrowth model (this may take several minutes)...")
    print(f"  minSupport=0.001 (~{int(basket_count * 0.001):,} orders)")
    print(f"  minConfidence=0.05")

    fpgrowth = FPGrowth(
        itemsCol="items",
        minSupport=0.001,
        minConfidence=0.05
    )
    model = fpgrowth.fit(baskets)

    # --- 4. Extract association rules ---
    rules = model.associationRules
    rule_count = rules.count()
    print(f"\nGenerated {rule_count:,} association rules")

    if rule_count == 0:
        print("WARNING: No rules generated. Consider lowering minSupport/minConfidence.")
        return False

    # Show top 10 rules by confidence
    print("\nTop 10 rules by confidence:")
    rules.orderBy(col("confidence").desc()).show(10, truncate=False)

    # Show top 10 rules by lift
    print("\nTop 10 rules by lift:")
    rules.orderBy(col("lift").desc()).show(10, truncate=False)

    # --- 5. Write to Iceberg Gold ---
    # Flatten the array columns for better queryability
    rules_flat = rules.select(
        col("antecedent").alias("antecedent_items"),
        col("consequent").alias("consequent_items"),
        col("confidence"),
        col("lift"),
        col("support")
    )

    iceberg_table = "iceberg.gold.market_basket_rules"
    print(f"\nWriting {rule_count:,} rules to {iceberg_table}...")

    rules_flat.writeTo(iceberg_table) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()

    print(f"Successfully wrote {rule_count:,} market basket rules to {iceberg_table}")

    # Summary stats
    print("\nRule statistics:")
    rules.select(
        col("confidence"),
        col("lift"),
        col("support")
    ).describe().show()

    # Check lift distribution (lift > 1 = real association, not random)
    strong_rules = rules.filter(col("lift") > 1.0).count()
    print(f"Rules with lift > 1.0 (meaningful associations): {strong_rules:,}")
    print(f"Rules with lift <= 1.0 (random/noise): {rule_count - strong_rules:,}")

    return True


def main():
    """Main execution function."""
    print("\n" + "=" * 80)
    print("INSTACART MARKET BASKET MINING (FPGrowth)")
    print("  NOTE: This is an OPTIONAL/BONUS step — not a blocking dependency.")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"S3 Bucket: {S3_BUCKET}")
    print("=" * 80 + "\n")

    spark = create_spark_session()

    try:
        success = run_fpgrowth(spark)

        if success:
            print("\n" + "=" * 80)
            print("MARKET BASKET MINING COMPLETED SUCCESSFULLY")
            print("=" * 80)
            return 0
        else:
            print("\n" + "=" * 80)
            print("MARKET BASKET MINING COMPLETED WITH WARNINGS")
            print("=" * 80)
            return 1

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        print(f"\nEnd Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
