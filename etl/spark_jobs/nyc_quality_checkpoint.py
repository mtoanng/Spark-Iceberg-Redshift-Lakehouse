"""Reconcile canonical Silver, quarantine, and Gold before publication."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from etl.spark_jobs.arguments import parse_arguments

args = parse_arguments(
    ["SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"],
    {
        "CATALOG_NAME": "glue_catalog",
        "SILVER_DATABASE": "silver",
        "GOLD_DATABASE": "gold",
        "OPS_DATABASE": "ops",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def _table(namespace: str, name: str) -> str:
    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    database = _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
    return f"{catalog}.{database}.{name}"


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    year, month = int(args["SOURCE_YEAR"]), int(args["SOURCE_MONTH"])
    run_id = args["INGESTION_RUN_ID"].replace("'", "''")
    manifest_table = _table("ops", "source_run_manifest")
    manifest = spark.sql(
        f"SELECT run_status, bronze_row_count, silver_row_count, quarantine_row_count "
        f"FROM {manifest_table} WHERE source_year={year} AND source_month={month} "
        f"AND ingestion_run_id='{run_id}' ORDER BY updated_at DESC LIMIT 1"
    ).first()
    if not manifest or manifest.run_status != "silver_published":
        raise ValueError("Reconciliation requires a silver_published manifest row.")
    silver_count = (
        spark.table(_table("silver", "silver_trips"))
        .filter((col("source_year") == year) & (col("source_month") == month))
        .count()
    )
    quarantine_count = (
        spark.table(_table("silver", "quarantine_trips"))
        .filter((col("_source_year") == year) & (col("_source_month") == month))
        .count()
    )
    gold_count = (
        spark.table(_table("gold", "fct_trips"))
        .filter((col("source_year") == year) & (col("source_month") == month))
        .count()
    )
    differences = {
        "bronze_vs_classified": int(manifest.bronze_row_count)
        - silver_count
        - quarantine_count,
        "manifest_silver": int(manifest.silver_row_count) - silver_count,
        "manifest_quarantine": int(manifest.quarantine_row_count) - quarantine_count,
        "gold_vs_silver": gold_count - silver_count,
    }
    if any(differences.values()):
        raise ValueError(f"Monthly reconciliation differences: {differences}")
    spark.sql(
        f"UPDATE {manifest_table} SET run_status='reconciled', gold_row_count={gold_count}, "
        f"publication_status='pending', updated_at=current_timestamp(), failure_stage=NULL, "
        f"failure_message=NULL WHERE source_year={year} AND source_month={month} "
        f"AND ingestion_run_id='{run_id}'"
    )


if __name__ == "__main__":
    main()
