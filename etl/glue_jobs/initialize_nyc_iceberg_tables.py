"""Remote-only Glue job that creates NYC Bronze and Silver Iceberg tables."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl.iceberg.catalog import TABLE_SPECS, namespace_ddl, table_ddl


args = getResolvedOptions(sys.argv, ["JOB_NAME", "WAREHOUSE_URI"])


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session

    for namespace in sorted({spec.namespace for spec in TABLE_SPECS}):
        spark.sql(namespace_ddl(namespace))
    for spec in TABLE_SPECS:
        spark.sql(table_ddl(spec, args["WAREHOUSE_URI"]))

    job.commit()


if __name__ == "__main__":
    main()
