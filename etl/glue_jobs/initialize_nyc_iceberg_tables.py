"""Remote-only Glue job that creates NYC Bronze and Silver Iceberg tables."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl.iceberg.catalog import (
    TABLE_SPECS,
    namespace_ddl,
    schema_evolution_ddl,
    table_ddl,
)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "WAREHOUSE_URI"])


def _optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session

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

    job.commit()


if __name__ == "__main__":
    main()
