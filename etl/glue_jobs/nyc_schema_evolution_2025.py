"""Remote-only, one-time schema evolution entry point for a 2025 month."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl.iceberg.lifecycle import plan_2025_hvfhs_schema_evolution


args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_YEAR", "SOURCE_MONTH"])


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    plan = plan_2025_hvfhs_schema_evolution(
        source_year=int(args["SOURCE_YEAR"]), source_month=int(args["SOURCE_MONTH"])
    )
    glue_context.spark_session.sql(plan.ddl)
    job.commit()


if __name__ == "__main__":
    main()
