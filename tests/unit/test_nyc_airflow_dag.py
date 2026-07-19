"""DAG import and topology test without launching an Airflow service on Windows."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py"


def _fake_airflow_modules(monkeypatch):
    current_dag: list[FakeDAG] = []

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
        "airflow": types.SimpleNamespace(DAG=FakeDAG),
        "airflow.models": types.SimpleNamespace(Variable=FakeVariable),
        "airflow.models.param": types.SimpleNamespace(Param=FakeParam),
        "airflow.operators.bash": types.SimpleNamespace(BashOperator=FakeOperator),
        "airflow.operators.python": types.SimpleNamespace(PythonOperator=FakeOperator),
        "airflow.operators.trigger_dagrun": types.SimpleNamespace(TriggerDagRunOperator=FakeOperator),
        "airflow.providers.amazon.aws.operators.glue": types.SimpleNamespace(GlueJobOperator=FakeOperator),
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
        "silver_transform",
        "dbt_build",
        "quality_checkpoint",
    ]
    assert monthly.params["year"].default == 2024
    assert monthly.params["month"].default == 1
    assert monthly.params["force"].default is False
    assert monthly.tasks[0].downstream_task_ids == {"bronze_ingestion"}
    assert monthly.tasks[3].downstream_task_ids == {"quality_checkpoint"}

    backfill = module.nyc_hvfhs_three_month_backfill_dag
    assert [task.task_id for task in backfill.tasks] == ["trigger_month_1", "trigger_month_2", "trigger_month_3"]
    assert backfill.tasks[0].downstream_task_ids == {"trigger_month_2"}
    assert backfill.tasks[1].downstream_task_ids == {"trigger_month_3"}
