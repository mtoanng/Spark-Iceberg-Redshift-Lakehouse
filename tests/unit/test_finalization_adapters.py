"""Credential-independent contracts for final Airflow-side adapters."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json

import pytest

from etl.orchestration.nyc_hvfhs_publication import publish_month
from etl.orchestration.nyc_hvfhs_reconciliation import (
    ReconciliationError,
    reconcile_month,
)
from etl.orchestration.nyc_hvfhs_verification import verify_month


class FakeRedshiftData:
    def __init__(self, records: list[list[list[dict[str, object]]]]) -> None:
        self.records = iter(records)
        self.by_id: dict[str, list[list[dict[str, object]]]] = {}

    def execute_statement(self, **_: object) -> dict[str, str]:
        statement_id = f"statement-{len(self.by_id) + 1}"
        self.by_id[statement_id] = next(self.records)
        return {"Id": statement_id}

    def describe_statement(self, *, Id: str) -> dict[str, str]:
        return {"Status": "FINISHED"}

    def get_statement_result(self, *, Id: str) -> dict[str, object]:
        return {"Records": self.by_id[Id]}


def _redshift_record(*values: object) -> list[dict[str, object]]:
    return [
        {"longValue": value} if isinstance(value, int) else {"stringValue": value}
        for value in values
    ]


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.puts = 0

    def put_object(self, **kwargs: object) -> None:
        self.puts += 1
        self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))] = dict(kwargs)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        item = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(bytes(item["Body"])),
            "Metadata": item.get("Metadata", {}),
        }

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
        "iceberg_snapshot_ids": {
            "bronze": "101",
            "silver": "102",
            "quarantine": "103",
        },
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


def _seed_dbt_artifact(s3: FakeS3) -> None:
    artifact = json.dumps(
        {"metadata": {"invocation_id": "dbt-run"}, "results": [{"status": "success"}]}
    ).encode()
    s3.objects[("bucket", "manifests/dbt-results.json")] = {
        "Body": artifact,
        "Metadata": {"sha256": hashlib.sha256(artifact).hexdigest()},
    }


def test_reconciliation_reads_all_evidence_in_one_redshift_statement() -> None:
    outcome = reconcile_month(
        source_year=2024,
        source_month=1,
        ingestion_run_id="stable-run",
        redshift_database="lakehouse",
        redshift_workgroup_name="serverless",
        redshift_client=FakeRedshiftData(
            [
                [
                    _redshift_record(
                        5,
                        1,
                        4,
                        1,
                        "silver_published",
                        5,
                        1,
                        4,
                        "101",
                        "102",
                        "103",
                    )
                ]
            ]
        ),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert outcome["bronze_equals_classified"] is True
    assert outcome["silver_equals_gold"] is True
    assert outcome["iceberg_snapshot_ids"]["silver"] == "102"
    assert outcome["redshift_statement_id"] == "statement-1"


def test_failed_reconciliation_invariant_raises_and_cannot_publish() -> None:
    with pytest.raises(ReconciliationError, match="Monthly reconciliation failed"):
        reconcile_month(
            source_year=2024,
            source_month=1,
            ingestion_run_id="stable-run",
            redshift_database="lakehouse",
            redshift_workgroup_name="serverless",
            redshift_client=FakeRedshiftData(
                [
                    [
                        _redshift_record(
                            5,
                            1,
                            3,
                            1,
                            "silver_published",
                            5,
                            1,
                            3,
                            "101",
                            "102",
                            "103",
                        )
                    ]
                ]
            ),
        )


def test_publication_reuses_identical_content_and_rejects_conflict() -> None:
    s3 = FakeS3()
    _seed_dbt_artifact(s3)
    kwargs = {
        "audit": _audit(),
        "reconciliation": _reconciliation(),
        "dbt_result_uri": "s3://bucket/manifests/dbt-results.json",
        "publication_prefix_uri": "s3://bucket/manifests",
        "redshift_database": "lakehouse",
        "s3_client": s3,
        "now": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    first = publish_month(**kwargs)
    second = publish_month(
        **{**kwargs, "now": datetime(2026, 1, 2, tzinfo=timezone.utc)}
    )
    assert first["publication_uri"] == second["publication_uri"]
    assert s3.puts == 1
    with pytest.raises(ValueError, match="conflicting"):
        publish_month(
            **{
                **kwargs,
                "reconciliation": {**_reconciliation(), "gold_row_count": 2},
            }
        )
    with pytest.raises(ValueError, match="identity"):
        publish_month(
            **{
                **kwargs,
                "reconciliation": {
                    **_reconciliation(),
                    "ingestion_run_id": "another-run",
                },
            }
        )


def test_verification_is_bounded_to_publication_silver_and_gold() -> None:
    s3 = FakeS3()
    _seed_dbt_artifact(s3)
    publication = publish_month(
        audit=_audit(),
        reconciliation=_reconciliation(),
        dbt_result_uri="s3://bucket/manifests/dbt-results.json",
        publication_prefix_uri="s3://bucket/manifests",
        redshift_database="lakehouse",
        s3_client=s3,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    outcome = verify_month(
        source_year=2024,
        source_month=1,
        ingestion_run_id="stable-run",
        publication=publication,
        redshift_database="lakehouse",
        redshift_workgroup_name="serverless",
        redshift_client=FakeRedshiftData([[_redshift_record(1, 1)]]),
        s3_client=s3,
    )
    assert outcome["silver_row_count"] == outcome["gold_row_count"] == 1
    assert outcome["redshift_statement_id"] == "statement-1"
