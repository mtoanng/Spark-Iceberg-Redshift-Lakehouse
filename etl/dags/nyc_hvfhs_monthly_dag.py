"""Manual Airflow 3 orchestration for the NYC HVFHV lakehouse.

The monthly DAG processes exactly one immutable month. The companion backfill
DAG triggers three such runs sequentially. Neither DAG contains transformation
logic; Glue, dbt, and the quality checkpoint own that work.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from etl.orchestration.nyc_hvfhs_runs import MonthlyRunRequest, audit_for_source
from etl.sources.nyc_hvfhs import SourceFile, monthly_trip_filename


MONTHLY_DAG_ID = "nyc_hvfhs_monthly"
BACKFILL_DAG_ID = "nyc_hvfhs_three_month_backfill"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _prepare_month(year: int, month: int, force: bool) -> dict[str, object]:
    """Resolve immutable source facts from Airflow Variables and return an audit."""

    request = MonthlyRunRequest(year=int(year), month=int(month), force=bool(force))
    filename = monthly_trip_filename(request.year, request.month)
    landing_uri = Variable.get("nyc_landing_uri").rstrip("/")
    checksum = Variable.get(f"nyc_hvfhs_{request.year}_{request.month:02d}_sha256")
    size_bytes = int(Variable.get(f"nyc_hvfhs_{request.year}_{request.month:02d}_size_bytes"))
    source = SourceFile(
        source_year=request.year,
        source_month=request.month,
        source_uri=f"{landing_uri}/{filename}",
        source_checksum=checksum,
        source_size_bytes=size_bytes,
    )
    audit = audit_for_source(request, source)
    return {
        "run_id": audit.run_id,
        "source_year": audit.source_year,
        "source_month": audit.source_month,
        "source_uri": audit.source_uri,
        "source_checksum": audit.source_checksum,
        "source_size_bytes": source.source_size_bytes,
        "force": audit.force,
        "taxi_zone_uri": Variable.get("nyc_taxi_zone_uri"),
        "taxi_zone_checksum": Variable.get("nyc_taxi_zone_sha256"),
    }


def _monthly_params() -> dict[str, Param]:
    return {
        "year": Param(2024, type="integer", minimum=2019, maximum=2099),
        "month": Param(1, type="integer", minimum=1, maximum=12),
        "force": Param(False, type="boolean"),
    }


with DAG(
    dag_id=MONTHLY_DAG_ID,
    description="Manual one-month NYC TLC HVFHV Bronze-to-Gold run with a quality gate.",
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
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
            "force": "{{ params.force }}",
        },
    )

    bronze_ingestion = GlueJobOperator(
        task_id="bronze_ingestion",
        job_name="{{ var.value.nyc_bronze_job_name }}",
        script_args={
            "--SOURCE_URI": "{{ ti.xcom_pull(task_ids='prepare_month')['source_uri'] }}",
            "--SOURCE_YEAR": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "--SOURCE_MONTH": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "--SOURCE_CHECKSUM": "{{ ti.xcom_pull(task_ids='prepare_month')['source_checksum'] }}",
            "--INGESTION_RUN_ID": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "--TAXI_ZONE_URI": "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_uri'] }}",
            "--TAXI_ZONE_CHECKSUM": "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_checksum'] }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        verbose=True,
    )

    silver_transform = GlueJobOperator(
        task_id="silver_transform",
        job_name="{{ var.value.nyc_silver_job_name }}",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        verbose=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd {{ var.value.nyc_project_root }}/etl/dbt_project && "
            "dbt build --profiles-dir . --target glue"
        ),
    )

    quality_checkpoint = GlueJobOperator(
        task_id="quality_checkpoint",
        job_name="{{ var.value.nyc_quality_checkpoint_job_name }}",
        script_args={
            "--SOURCE_YEAR": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "--SOURCE_MONTH": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        verbose=True,
    )

    prepare_month >> bronze_ingestion >> silver_transform >> dbt_build >> quality_checkpoint


def _backfill_params() -> dict[str, Param]:
    return {
        "year": Param(2024, type="integer", minimum=2019, maximum=2099),
        "month": Param(1, type="integer", minimum=1, maximum=10),
        "force": Param(False, type="boolean"),
    }


with DAG(
    dag_id=BACKFILL_DAG_ID,
    description="Manually trigger three consecutive NYC HVFHV monthly runs in order.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=_backfill_params(),
    render_template_as_native_obj=True,
    tags=["nyc", "hvfhs", "iceberg", "manual", "backfill"],
) as nyc_hvfhs_three_month_backfill_dag:
    trigger_month_1 = TriggerDagRunOperator(
        task_id="trigger_month_1",
        trigger_dag_id=MONTHLY_DAG_ID,
        conf={"year": "{{ params.year }}", "month": "{{ params.month }}", "force": "{{ params.force }}"},
        wait_for_completion=True,
    )
    trigger_month_2 = TriggerDagRunOperator(
        task_id="trigger_month_2",
        trigger_dag_id=MONTHLY_DAG_ID,
        conf={
            "year": "{{ params.year }}",
            "month": "{{ (params.month | int) + 1 }}",
            "force": "{{ params.force }}",
        },
        wait_for_completion=True,
    )
    trigger_month_3 = TriggerDagRunOperator(
        task_id="trigger_month_3",
        trigger_dag_id=MONTHLY_DAG_ID,
        conf={
            "year": "{{ params.year }}",
            "month": "{{ (params.month | int) + 2 }}",
            "force": "{{ params.force }}",
        },
        wait_for_completion=True,
    )

    trigger_month_1 >> trigger_month_2 >> trigger_month_3


nyc_hvfhs_monthly_dag.doc_md = """
# NYC HVFHV monthly orchestration

Manually trigger this DAG with `year`, `month`, and `force`. `force` only
requests a retry of the same immutable source identity; a changed checksum must
be blocked by the manifest contract. The quality checkpoint is the promotion
gate after dbt Gold.
"""
