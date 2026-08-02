"""Apply the one approved post-baseline Iceberg schema change."""

from pyspark.sql import SparkSession

from etl.iceberg.catalog import schema_evolution_ddl
from etl.spark_jobs.arguments import parse_arguments


args = parse_arguments(
    [],
    {
        "CATALOG_NAME": "glue_catalog",
        "BRONZE_DATABASE": "bronze",
        "SILVER_DATABASE": "silver",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    for ddl in schema_evolution_ddl(
        catalog=catalog,
        bronze_database=_optional_arg("BRONZE_DATABASE", "bronze"),
        silver_database=_optional_arg("SILVER_DATABASE", "silver"),
    ):
        spark.sql(ddl)


if __name__ == "__main__":
    main()
