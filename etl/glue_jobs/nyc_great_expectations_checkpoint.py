"""Mandatory Great Expectations checkpoint between Bronze and Silver."""

import json
import sys

import great_expectations as gx
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

from etl.sources.nyc_hvfhs import BASE_REQUIRED_TRIP_COLUMNS


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"]
)


def _optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def _table(namespace: str, name: str) -> str:
    return f"{_optional_arg('CATALOG_NAME', 'glue_catalog')}.{_optional_arg(f'{namespace.upper()}_DATABASE', namespace)}.{name}"


BRONZE_TRIPS_TABLE = _table("bronze", "bronze_hvfhs_trips")
MANIFEST_TABLE = _table("ops", "source_run_manifest")


def _suite() -> gx.ExpectationSuite:
    return gx.ExpectationSuite(
        name="nyc_hvfhs_bronze_pre_silver",
        expectations=[
            gx.expectations.ExpectColumnToExist(column=name)
            for name in sorted(BASE_REQUIRED_TRIP_COLUMNS)
        ],
    )


def _persist(spark, status: str, summary: str) -> None:
    year, month, run_id = (
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["INGESTION_RUN_ID"].replace("'", "''"),
    )
    escaped_summary = summary.replace("'", "''")[:4000]
    spark.sql(
        f"UPDATE {MANIFEST_TABLE} SET run_status='{status}', validation_status='{('passed' if status == 'ge_passed' else 'blocked')}', validation_result_summary='{escaped_summary}', updated_at=current_timestamp(), failure_stage={('NULL' if status == 'ge_passed' else "'great_expectations'")}, failure_message={('NULL' if status == 'ge_passed' else "'Blocking Great Expectations expectation failed.'")} WHERE source_year={year} AND source_month={month} AND ingestion_run_id='{run_id}'"
    )


def main() -> None:
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark = glue_context.spark_session
    year, month, run_id = (
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["INGESTION_RUN_ID"],
    )
    escaped_run_id = run_id.replace("'", "''")
    manifest = spark.sql(
        f"SELECT run_status FROM {MANIFEST_TABLE} WHERE source_year={year} AND source_month={month} AND ingestion_run_id='{escaped_run_id}' ORDER BY updated_at DESC LIMIT 1"
    ).first()
    if not manifest or manifest.run_status != "bronze_published":
        raise ValueError(
            "Great Expectations requires the requested Bronze run to be published."
        )
    bronze = spark.table(BRONZE_TRIPS_TABLE).filter(
        (col("_source_year") == year)
        & (col("_source_month") == month)
        & (col("_ingestion_run_id") == run_id)
    )
    # GX performs the blocking structural suite against the actual month frame.
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name="bronze_month")
    asset = datasource.add_dataframe_asset(name="hvfhs_month")
    batch = asset.add_batch_definition_whole_dataframe("month").get_batch(
        batch_parameters={"dataframe": bronze}
    )
    validation = batch.validate(expectation_suite=_suite())
    blocking_success = bool(validation.success) and bronze.limit(1).count() == 1
    summary = json.dumps(
        {
            "suite": "nyc_hvfhs_bronze_pre_silver",
            "gx_success": bool(validation.success),
            "blocking_success": blocking_success,
            "scope": "required_columns_and_non_empty_month",
        },
        sort_keys=True,
    )
    _persist(spark, "ge_passed" if blocking_success else "ge_blocked", summary)
    if not blocking_success:
        raise ValueError(
            "Great Expectations blocking checkpoint failed; Silver will not run."
        )
    job.commit()


if __name__ == "__main__":
    main()
