"""Mandatory Great Expectations checkpoint between Bronze and Silver."""

import json
import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from etl.contracts.nyc_hvfhs_identity import required_identity_columns
from etl.sources.nyc_hvfhs import required_trip_columns
from etl.spark_jobs.arguments import parse_arguments


args = parse_arguments(
    ["SOURCE_YEAR", "SOURCE_MONTH", "INGESTION_RUN_ID"],
    {
        "CATALOG_NAME": "glue_catalog",
        "BRONZE_DATABASE": "bronze",
        "OPS_DATABASE": "ops",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def _table(namespace: str, name: str) -> str:
    return f"{_optional_arg('CATALOG_NAME', 'glue_catalog')}.{_optional_arg(f'{namespace.upper()}_DATABASE', namespace)}.{name}"


BRONZE_TRIPS_TABLE = _table("bronze", "bronze_hvfhs_trips")
MANIFEST_TABLE = _table("ops", "source_run_manifest")


def _suite(year: int) -> gx.ExpectationSuite:
    required = required_trip_columns(year) | required_identity_columns(year)
    return gx.ExpectationSuite(
        name="nyc_hvfhs_bronze_pre_silver",
        expectations=[
            gx.expectations.ExpectColumnToExist(column=name)
            for name in sorted(required)
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
    spark = SparkSession.builder.getOrCreate()
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
    validation = batch.validate(expectation_suite=_suite(year))
    blocking_success = bool(validation.success) and bronze.limit(1).count() == 1
    summary = json.dumps(
        {
            "suite": "nyc_hvfhs_bronze_pre_silver",
            "gx_success": bool(validation.success),
            "blocking_success": blocking_success,
            "scope": "required_columns_non_empty_month_identity_inputs",
        },
        sort_keys=True,
    )
    _persist(spark, "ge_passed" if blocking_success else "ge_blocked", summary)
    if not blocking_success:
        raise ValueError(
            "Great Expectations blocking checkpoint failed; Silver will not run."
        )


if __name__ == "__main__":
    main()
