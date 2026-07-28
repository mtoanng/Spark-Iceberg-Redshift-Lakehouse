"""Airflow 3 orchestration for the bounded NYC HVFHV lakehouse."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
from urllib.parse import urlparse

import boto3
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, Variable

from etl.contracts.nyc_hvfhs_identity import identity_policy_version
from etl.orchestration.nyc_hvfhs_dbt import require_dbt_result_artifact
from etl.orchestration.nyc_hvfhs_publication import publish_month
from etl.orchestration.nyc_hvfhs_reconciliation import reconcile_month
from etl.orchestration.nyc_hvfhs_runs import (
    MonthlyRunRequest,
    audit_for_source,
    sequential_backfill_requests,
)
from etl.orchestration.nyc_hvfhs_verification import verify_month
from etl.sources.nyc_hvfhs import (
    SourceFile,
    monthly_trip_filename,
    validate_landed_source,
)


MONTHLY_DAG_ID = "nyc_hvfhs_monthly"
BACKFILL_DAG_ID = "nyc_hvfhs_four_month_backfill"
DBT_PROJECT_PATH = Path(__file__).resolve().parents[1] / "dbt_project"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

EMR_APPLICATION_ID = "{{ var.value.nyc_emr_serverless_application_id }}"
EMR_EXECUTION_ROLE_ARN = "{{ var.value.nyc_emr_serverless_execution_role_arn }}"
EMR_SCRIPT_PREFIX_URI = "{{ var.value.nyc_spark_script_prefix_uri }}"
EMR_PACKAGE_URI = "{{ var.value.nyc_spark_package_uri }}"
EMR_LOG_URI = "{{ var.value.nyc_emr_serverless_log_uri }}"


def _emr_spark_job(script_name: str, arguments: list[str]) -> dict[str, object]:
    return {
        "application_id": EMR_APPLICATION_ID,
        "execution_role_arn": EMR_EXECUTION_ROLE_ARN,
        "job_driver": {
            "sparkSubmit": {
                "entryPoint": f"{EMR_SCRIPT_PREFIX_URI}/{script_name}",
                "entryPointArguments": arguments,
                "sparkSubmitParameters": (
                    f"--py-files {EMR_PACKAGE_URI} "
                    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.defaultCatalog=glue_catalog "
                    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.glue_catalog.warehouse={{ var.value.nyc_warehouse_uri }} "
                    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
                ),
            }
        },
        "configuration_overrides": {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": EMR_LOG_URI}
            }
        },
        "aws_conn_id": None,
        "wait_for_completion": True,
    }


def _s3_identity(uri: str) -> tuple[str, int]:
    """Read upstream-provided immutable identity from a landed object."""

    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError(f"Expected a complete S3 URI, got {uri!r}")
    head = boto3.client("s3").head_object(Bucket=parsed.netloc, Key=key)
    checksum = str(head.get("Metadata", {}).get("sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError(f"Landed object is missing SHA-256 metadata: {uri}")
    size = int(head.get("ContentLength", 0))
    if size <= 0:
        raise ValueError(f"Landed object is empty: {uri}")
    return checksum, size


def _prepare_month(year: int, month: int) -> dict[str, object]:
    """Bind a requested month to the object identity already landed in S3."""

    request = MonthlyRunRequest(year=int(year), month=int(month))
    source_uri = (
        f"{Variable.get('nyc_landing_uri').rstrip('/')}/"
        f"{monthly_trip_filename(request.year, request.month)}"
    )
    checksum, size_bytes = _s3_identity(source_uri)
    source = SourceFile(
        source_year=request.year,
        source_month=request.month,
        source_uri=source_uri,
        source_checksum=checksum,
        source_size_bytes=size_bytes,
    )
    validate_landed_source(source)
    audit = audit_for_source(request, source)
    taxi_zone_uri = Variable.get("nyc_taxi_zone_uri")
    taxi_zone_checksum, _ = _s3_identity(taxi_zone_uri)
    return {
        "run_id": audit.run_id,
        "source_year": audit.source_year,
        "source_month": audit.source_month,
        "source_uri": audit.source_uri,
        "source_checksum": audit.source_checksum,
        "source_size_bytes": audit.source_size_bytes,
        "identity_policy_version": identity_policy_version(request.year),
        "taxi_zone_uri": taxi_zone_uri,
        "taxi_zone_checksum": taxi_zone_checksum,
    }


def _monthly_params() -> dict[str, Param]:
    return {
        "year": Param(2024, type="integer", minimum=2019, maximum=2099),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    }


with DAG(
    dag_id=MONTHLY_DAG_ID,
    description="One immutable NYC TLC month from Bronze through published Gold.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=_monthly_params(),
    render_template_as_native_obj=True,
    tags=["nyc", "hvfhs", "iceberg", "manual"],
) as nyc_hvfhs_monthly_dag:
    prepare_month = PythonOperator(
        task_id="prepare_month",
        python_callable=_prepare_month,
        op_kwargs={"year": "{{ params.year }}", "month": "{{ params.month }}"},
    )

    bronze_ingestion = EmrServerlessStartJobOperator(
        task_id="bronze_ingestion_emr",
        **_emr_spark_job(
            "nyc_bronze_ingestion.py",
            [
                "--SOURCE_URI",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_uri'] }}",
                "--SOURCE_YEAR",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
                "--SOURCE_MONTH",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
                "--SOURCE_CHECKSUM",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_checksum'] }}",
                "--INGESTION_RUN_ID",
                "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
                "--TAXI_ZONE_URI",
                "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_uri'] }}",
                "--TAXI_ZONE_CHECKSUM",
                "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_checksum'] }}",
                "--SOURCE_SIZE_BYTES",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_size_bytes'] }}",
            ],
        ),
    )

    silver_transform = EmrServerlessStartJobOperator(
        task_id="silver_transform_emr",
        **_emr_spark_job(
            "nyc_silver_transform.py",
            [
                "--SOURCE_YEAR",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
                "--SOURCE_MONTH",
                "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
                "--INGESTION_RUN_ID",
                "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            ],
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="python -m etl.orchestration.nyc_hvfhs_dbt",
        append_env=True,
        env={
            "DBT_PROJECT_PATH": str(DBT_PROJECT_PATH),
            "DBT_PUBLICATION_PREFIX_URI": "{{ var.value.nyc_publication_prefix_uri }}",
            "DBT_SOURCE_YEAR": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "DBT_SOURCE_MONTH": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "DBT_RUN_ID": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "REDSHIFT_HOST": "{{ var.value.redshift_host }}",
            "REDSHIFT_WORKGROUP_NAME": "{{ var.value.redshift_workgroup_name }}",
            "REDSHIFT_DATABASE": "{{ var.value.redshift_database }}",
            "AWS_ACCOUNT_ID": "{{ var.value.aws_account_id }}",
            "AWS_REGION": "{{ var.value.aws_region }}",
        },
    )

    dbt_result_artifact = PythonOperator(
        task_id="dbt_result_artifact",
        python_callable=require_dbt_result_artifact,
        op_kwargs={
            "publication_prefix_uri": "{{ var.value.nyc_publication_prefix_uri }}",
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
        },
    )

    reconciliation = PythonOperator(
        task_id="reconciliation",
        python_callable=reconcile_month,
        op_kwargs={
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "ingestion_run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "athena_workgroup": "{{ var.value.athena_workgroup }}",
            "redshift_database": "{{ var.value.redshift_database }}",
            "redshift_workgroup_name": "{{ var.value.redshift_workgroup_name }}",
        },
    )

    publication_manifest = PythonOperator(
        task_id="publication_manifest",
        python_callable=publish_month,
        op_kwargs={
            "audit": "{{ ti.xcom_pull(task_ids='prepare_month') }}",
            "reconciliation": "{{ ti.xcom_pull(task_ids='reconciliation') }}",
            "dbt_result_uri": "{{ ti.xcom_pull(task_ids='dbt_result_artifact') }}",
            "publication_prefix_uri": "{{ var.value.nyc_publication_prefix_uri }}",
            "redshift_database": "{{ var.value.redshift_database }}",
        },
    )

    verification = PythonOperator(
        task_id="verification",
        python_callable=verify_month,
        op_kwargs={
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "ingestion_run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "publication": "{{ ti.xcom_pull(task_ids='publication_manifest') }}",
            "athena_workgroup": "{{ var.value.athena_workgroup }}",
            "redshift_database": "{{ var.value.redshift_database }}",
            "redshift_workgroup_name": "{{ var.value.redshift_workgroup_name }}",
        },
    )

    (
        prepare_month
        >> bronze_ingestion
        >> silver_transform
        >> dbt_build
        >> dbt_result_artifact
        >> reconciliation
        >> publication_manifest
        >> verification
    )


with DAG(
    dag_id=BACKFILL_DAG_ID,
    description="Trigger four bounded monthly runs sequentially.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=_monthly_params(),
    render_template_as_native_obj=True,
    tags=["nyc", "hvfhs", "iceberg", "manual", "backfill"],
) as nyc_hvfhs_four_month_backfill_dag:

    def _prepare_backfill(year: int, month: int) -> list[dict[str, int]]:
        return [
            {"year": request.year, "month": request.month}
            for request in sequential_backfill_requests(int(year), int(month))
        ]

    prepare_backfill = PythonOperator(
        task_id="prepare_backfill",
        python_callable=_prepare_backfill,
        op_kwargs={"year": "{{ params.year }}", "month": "{{ params.month }}"},
    )
    triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_month_{index + 1}",
            trigger_dag_id=MONTHLY_DAG_ID,
            conf=f"{{{{ ti.xcom_pull(task_ids='prepare_backfill')[{index}] }}}}",
            wait_for_completion=True,
        )
        for index in range(4)
    ]
    prepare_backfill >> triggers[0] >> triggers[1] >> triggers[2] >> triggers[3]


nyc_hvfhs_monthly_dag.doc_md = """
# NYC HVFHV monthly orchestration

Trigger with `year` and `month`. The same immutable object identity may be
rerun safely; a changed URI, SHA-256, or byte size is rejected. Bronze owns
source checks, Silver owns validation/quarantine, dbt owns Gold tests, and
reconciliation owns the two cross-layer count invariants.
"""
