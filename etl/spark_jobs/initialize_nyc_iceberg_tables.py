"""EMR Serverless PySpark entrypoint that creates NYC Iceberg tables."""

from pyspark.sql import SparkSession

from etl.iceberg.catalog import (
    TABLE_SPECS,
    namespace_ddl,
    schema_evolution_ddl,
    table_ddl,
)
from etl.spark_jobs.arguments import parse_arguments


args = parse_arguments(
    ["WAREHOUSE_URI"],
    {
        "CATALOG_NAME": "glue_catalog",
        "BRONZE_DATABASE": "bronze",
        "SILVER_DATABASE": "silver",
        "OPS_DATABASE": "ops",
        "GOLD_DATABASE": "gold",
        "APPLY_2025_EVOLUTION": "false",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    namespace_map = {
        namespace: _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
        for namespace in {spec.namespace for spec in TABLE_SPECS}
    }
    for namespace in sorted(namespace_map.values()):
        spark.sql(namespace_ddl(namespace, catalog=catalog))
    for spec in TABLE_SPECS:
        mapped = type(spec)(
            namespace_map[spec.namespace], spec.name, spec.columns, spec.partitioned_by
        )
        spark.sql(table_ddl(mapped, args["WAREHOUSE_URI"], catalog=catalog))
    if _optional_arg("APPLY_2025_EVOLUTION", "false").lower() == "true":
        for ddl in schema_evolution_ddl(
            catalog=catalog,
            bronze_database=namespace_map["bronze"],
            silver_database=namespace_map["silver"],
        ):
            spark.sql(ddl)


if __name__ == "__main__":
    main()
