"""DAG import and topology test without launching an Airflow service on Windows."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py"


def _fake_airflow_modules(monkeypatch):
    current_dag: list[FakeDAG] = []

    def fake_package(name: str):
        package = types.ModuleType(name)
        package.__path__ = []
        return package

    class FakeParam:
        def __init__(self, default, **kwargs):
            self.default = default
            self.kwargs = kwargs

    class FakeDAG:
        def __init__(self, *, dag_id, params, **kwargs):
            self.dag_id = dag_id
            self.params = params
            self.kwargs = kwargs
            self.tasks = []

        def __enter__(self):
            current_dag.append(self)
            return self

        def __exit__(self, *_):
            current_dag.pop()

    class FakeOperator:
        def __init__(self, *, task_id, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.downstream_task_ids = set()
            current_dag[-1].tasks.append(self)

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            return other

    class FakeVariable:
        @staticmethod
        def get(_):
            raise AssertionError("DAG import must not resolve Airflow Variables")

    modules = {
        "airflow": fake_package("airflow"),
        "airflow.providers": fake_package("airflow.providers"),
        "airflow.providers.standard": fake_package("airflow.providers.standard"),
        "airflow.providers.standard.operators": fake_package(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.amazon": fake_package("airflow.providers.amazon"),
        "airflow.providers.amazon.aws": fake_package("airflow.providers.amazon.aws"),
        "airflow.providers.amazon.aws.operators": fake_package(
            "airflow.providers.amazon.aws.operators"
        ),
        "airflow.sdk": types.SimpleNamespace(
            DAG=FakeDAG, Param=FakeParam, Variable=FakeVariable
        ),
        "airflow.providers.standard.operators.python": types.SimpleNamespace(
            PythonOperator=FakeOperator
        ),
        "airflow.providers.standard.operators.bash": types.SimpleNamespace(
            BashOperator=FakeOperator
        ),
        "airflow.providers.standard.operators.trigger_dagrun": types.SimpleNamespace(
            TriggerDagRunOperator=FakeOperator
        ),
        "airflow.providers.amazon.aws.operators.emr": types.SimpleNamespace(
            EmrServerlessStartJobOperator=FakeOperator
        ),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_airflow_dag_import_and_manual_topology(monkeypatch) -> None:
    _fake_airflow_modules(monkeypatch)
    spec = importlib.util.spec_from_file_location("phase5_test_dag", DAG_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monthly = module.nyc_hvfhs_monthly_dag
    assert monthly.dag_id == "nyc_hvfhs_monthly"
    assert [task.task_id for task in monthly.tasks] == [
        "prepare_month",
        "bronze_ingestion_emr",
        "silver_transform_emr",
        "dbt_build",
        "dbt_result_artifact",
        "reconciliation",
        "publication_manifest",
        "verification",
    ]
    assert monthly.params["year"].default == 2024
    assert monthly.params["month"].default == 1
    assert set(monthly.params) == {"year", "month"}
    assert monthly.tasks[0].downstream_task_ids == {"bronze_ingestion_emr"}
    assert monthly.tasks[1].downstream_task_ids == {"silver_transform_emr"}
    assert all(monthly.tasks[index].kwargs["aws_conn_id"] is None for index in (1, 2))
    assert all(
        monthly.tasks[index].kwargs["application_id"]
        == "{{ var.value.nyc_emr_serverless_application_id }}"
        for index in (1, 2)
    )
    assert monthly.tasks[2].downstream_task_ids == {"dbt_build"}
    assert monthly.tasks[3].downstream_task_ids == {"dbt_result_artifact"}
    assert monthly.tasks[4].downstream_task_ids == {"reconciliation"}
    assert monthly.tasks[5].downstream_task_ids == {"publication_manifest"}
    assert monthly.tasks[6].downstream_task_ids == {"verification"}
    assert all(
        "trigger_rule" not in monthly.tasks[index].kwargs for index in (4, 5, 6, 7)
    )
    assert monthly.tasks[5].kwargs["python_callable"].__name__ == "reconcile_month"
    assert monthly.tasks[6].kwargs["python_callable"].__name__ == "publish_month"
    assert monthly.tasks[7].kwargs["python_callable"].__name__ == "verify_month"
    dbt_task = monthly.tasks[3]
    assert dbt_task.kwargs["bash_command"] == (
        "python -m etl.orchestration.nyc_hvfhs_dbt"
    )
    assert dbt_task.kwargs["append_env"] is True
    assert {"DBT_SOURCE_YEAR", "DBT_SOURCE_MONTH", "DBT_RUN_ID"} <= set(
        dbt_task.kwargs["env"]
    )

    backfill = module.nyc_hvfhs_four_month_backfill_dag
    assert [task.task_id for task in backfill.tasks] == [
        "prepare_backfill",
        "trigger_month_1",
        "trigger_month_2",
        "trigger_month_3",
        "trigger_month_4",
    ]
    assert backfill.params["month"].kwargs["maximum"] == 12
    assert backfill.tasks[0].downstream_task_ids == {"trigger_month_1"}
    assert backfill.tasks[1].downstream_task_ids == {"trigger_month_2"}
    assert backfill.tasks[2].downstream_task_ids == {"trigger_month_3"}
    assert backfill.tasks[3].downstream_task_ids == {"trigger_month_4"}
