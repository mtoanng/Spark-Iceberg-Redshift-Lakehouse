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
        "airflow.providers.standard.operators.bash": types.SimpleNamespace(
            BashOperator=FakeOperator
        ),
        "airflow.providers.standard.operators.python": types.SimpleNamespace(
            PythonOperator=FakeOperator
        ),
        "airflow.providers.standard.operators.trigger_dagrun": types.SimpleNamespace(
            TriggerDagRunOperator=FakeOperator
        ),
        "airflow.providers.amazon.aws.operators.glue": types.SimpleNamespace(
            GlueJobOperator=FakeOperator
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
        "bronze_ingestion",
        "great_expectations_checkpoint",
        "silver_transform",
        "dbt_build",
        "reconciliation",
        "publication_manifest",
        "athena_smoke",
    ]
    assert monthly.params["year"].default == 2024
    assert monthly.params["month"].default == 1
    assert monthly.params["force"].default is False
    assert monthly.tasks[0].downstream_task_ids == {"bronze_ingestion"}
    assert monthly.tasks[1].downstream_task_ids == {"great_expectations_checkpoint"}
    assert all(
        monthly.tasks[index].kwargs["aws_conn_id"] is None
        for index in (1, 2, 3, 5, 6)
    )
    assert monthly.tasks[2].downstream_task_ids == {"silver_transform"}
    assert monthly.tasks[4].downstream_task_ids == {"reconciliation"}
    assert monthly.tasks[5].downstream_task_ids == {"publication_manifest"}
    assert monthly.tasks[6].downstream_task_ids == {"athena_smoke"}
    assert "source_year" in monthly.tasks[4].kwargs["bash_command"]
    assert "source_month" in monthly.tasks[4].kwargs["bash_command"]
    assert "--max-scanned-bytes" in monthly.tasks[7].kwargs["bash_command"]
    assert "AWS_REGION" in monthly.tasks[7].kwargs["env"]

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
