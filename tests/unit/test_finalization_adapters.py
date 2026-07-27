"""Credential-independent contracts for final Airflow-side adapters."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json

import pytest

from athena.query_runner import AthenaQueryResult
from etl.orchestration.nyc_hvfhs_publication import publish_month
from etl.orchestration.nyc_hvfhs_reconciliation import (
    ReconciliationError,
    reconcile_month,
)
from etl.orchestration.nyc_hvfhs_verification import verify_month


class FakeAthenaRunner:
    def __init__(self, values: list[str]) -> None:
        self.values = iter(values)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def run(self, sql: str, **kwargs: object) -> AthenaQueryResult:
        self.calls.append((sql, kwargs))
        value = next(self.values)
        return AthenaQueryResult(
            query_execution_id=f"athena-{len(self.calls)}",
            columns=("count",),
            rows=((value,),),
            data_scanned_bytes=1,
            engine_execution_time_ms=1,
            result_location="s3://bucket/athena-results/result.csv",
        )


class FakeRedshiftData:
    def __init__(self, records: list[list[list[dict[str, object]]]]) -> None:
        self.records = iter(records)
        self.by_id: dict[str, list[list[dict[str, object]]]] = {}
        self.sql: list[str] = []

    def execute_statement(self, **kwargs: object) -> dict[str, str]:
        statement_id = f"statement-{len(self.sql) + 1}"
        self.sql.append(str(kwargs["Sql"]))
        self.by_id[statement_id] = next(self.records)
        return {"Id": statement_id}

    def describe_statement(self, *, Id: str) -> dict[str, str]:
        return {"Status": "FINISHED"}

    def get_statement_result(self, *, Id: str) -> dict[str, object]:
        return {"Records": self.by_id[Id]}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.puts = 0

    def put_object(self, **kwargs: object) -> None:
        self.puts += 1
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = dict(kwargs)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        item = self.objects[(Bucket, Key)]
        return {"Metadata": item.get("Metadata", {})}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        item = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(bytes(item["Body"]))}


def _reconciliation() -> dict[str, object]:
    return {
        "source_year": 2024,
        "source_month": 1,
        "ingestion_run_id": "stable-run",
        "bronze_row_count": 5,
        "silver_row_count": 1,
        "quarantine_row_count": 4,
        "gold_row_count": 1,
        "bronze_equals_classified": True,
        "silver_equals_gold": True,
        "athena_query_execution_ids": {},
        "redshift_statement_id": "statement-1",
        "reconciled_at": "2026-01-01T00:00:00+00:00",
    }


def _audit() -> dict[str, object]:
    return {
        "run_id": "stable-run",
        "source_uri": "s3://bucket/landing/fhvhv_tripdata_2024-01.parquet",
        "source_checksum": "a" * 64,
        "source_size_bytes": 123,
        "source_year": 2024,
        "source_month": 1,
        "identity_policy_version": "nyc-hvfhv-row-v1-2024",
    }


def test_reconciliation_uses_athena_open_layers_and_redshift_data_api() -> None:
    outcome = reconcile_month(
        source_year=2024,
        source_month=1,
        ingestion_run_id="stable-run",
        athena_workgroup="wg",
        redshift_database="lakehouse",
        redshift_workgroup_name="serverless",
        athena_runner=FakeAthenaRunner(["5", "1", "4"]),
        redshift_client=FakeRedshiftData([[[{"longValue": 1}]]]),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert outcome["bronze_equals_classified"] is True
    assert outcome["silver_equals_gold"] is True
    assert outcome["athena_query_execution_ids"] == {
        "bronze": "athena-1",
        "silver": "athena-2",
        "quarantine": "athena-3",
    }
    assert outcome["redshift_statement_id"] == "statement-1"


def test_failed_reconciliation_invariant_raises_and_cannot_publish() -> None:
    with pytest.raises(ReconciliationError, match="Monthly reconciliation failed"):
        reconcile_month(
            source_year=2024,
            source_month=1,
            ingestion_run_id="stable-run",
            athena_workgroup="wg",
            redshift_database="lakehouse",
            redshift_workgroup_name="serverless",
            athena_runner=FakeAthenaRunner(["5", "1", "3"]),
            redshift_client=FakeRedshiftData([[[{"longValue": 1}]]]),
        )


def test_publication_is_redshift_aware_and_rejects_conflicting_repeat() -> None:
    s3 = FakeS3()
    artifact = json.dumps(
        {"metadata": {"invocation_id": "dbt-run"}, "results": [{"status": "success"}]}
    ).encode()
    s3.objects[("bucket", "manifests/dbt-results.json")] = {
        "Body": artifact,
        "Metadata": {"sha256": hashlib.sha256(artifact).hexdigest()},
    }
    kwargs = {
        "audit": _audit(),
        "reconciliation": _reconciliation(),
        "dbt_result_uri": "s3://bucket/manifests/dbt-results.json",
        "publication_prefix_uri": "s3://bucket/manifests",
        "athena_workgroup": "wg",
        "redshift_database": "lakehouse",
        "athena_runner": FakeAthenaRunner(["101", "102", "103"]),
        "s3_client": s3,
        "now": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    first = publish_month(**kwargs)
    second = publish_month(
        **{
            **kwargs,
            "athena_runner": FakeAthenaRunner(["101", "102", "103"]),
            "now": datetime(2026, 1, 2, tzinfo=timezone.utc),
        }
    )
    assert first["publication_uri"] == second["publication_uri"]
    assert s3.puts == 1
    with pytest.raises(ValueError, match="conflicting"):
        publish_month(
            **{
                **kwargs,
                "athena_runner": FakeAthenaRunner(["101", "102", "103"]),
                "reconciliation": {**_reconciliation(), "gold_row_count": 2},
            }
        )


def test_verification_reads_open_layers_with_athena_and_gold_with_redshift() -> None:
    relations = [
        [{"stringValue": relation}]
        for relation in (
            "dim_date",
            "dim_operator",
            "dim_zone",
            "fct_trips",
            "mart_hourly_zone_demand",
            "mart_operator_metrics",
        )
    ]
    outcome = verify_month(
        source_year=2024,
        source_month=1,
        ingestion_run_id="stable-run",
        reconciliation=_reconciliation(),
        athena_workgroup="wg",
        redshift_database="lakehouse",
        redshift_workgroup_name="serverless",
        athena_runner=FakeAthenaRunner(["5", "1", "4"]),
        redshift_client=FakeRedshiftData(
            [
                relations,
                [[{"longValue": 1}]],
                [[{"longValue": 1}]],
                [[{"longValue": 1}]],
            ]
        ),
    )
    assert outcome["gold_row_count"] == 1
    assert set(outcome["athena_query_execution_ids"]) == {
        "bronze",
        "silver",
        "quarantine",
    }
    assert set(outcome["redshift_statement_ids"]) == {
        "relations",
        "fct_trips",
        "mart_hourly_zone_demand",
        "mart_operator_metrics",
    }
