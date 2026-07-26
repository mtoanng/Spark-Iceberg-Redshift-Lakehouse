"""Deterministic publication, rerun, reconciliation, and evolution contracts."""

from __future__ import annotations

import pytest

from etl.publication.nyc_hvfhs import (
    REQUIRED_GOLD_TABLES,
    TablePublication,
    build_publication_document,
    canonical_json,
)
from scripts.reconcile_outputs import reconcile
from scripts.verify_monthly_rerun import compare_monthly_evidence
from scripts.verify_schema_evolution import verify


def _publication():
    return build_publication_document(
        source={
            "source_uri": "s3://bucket/landing/fhvhv_tripdata_2024-01.parquet",
            "source_checksum": "a" * 64,
            "source_size_bytes": 100,
            "source_year": 2024,
            "source_month": 1,
        },
        ingestion_run_id="fhvhv-2024-01-aaaaaaaaaaaaaaaa",
        identity_policy_version="nyc-hvfhv-row-v1-2024",
        published_at="2026-07-26T00:00:00+00:00",
        bronze={"row_count": 5, "snapshot_id": "101"},
        silver={"row_count": 1, "snapshot_id": "102"},
        quarantine={"row_count": 4, "snapshot_id": "103"},
        gold_tables=[
            TablePublication(name, f"s3://bucket/gold/{name}", 1, str(200 + index))
            for index, name in enumerate(REQUIRED_GOLD_TABLES)
        ],
        dbt_summary={
            "status": "succeeded",
            "invocation_id": "dbt-invocation",
            "run_results_uri": "s3://bucket/manifests/dbt-results.json",
            "model_count": 6,
        },
    )


def test_publication_is_complete_and_byte_deterministic() -> None:
    first = _publication()
    second = _publication()
    assert canonical_json(first) == canonical_json(second)
    assert set(first["gold_tables"]) == set(REQUIRED_GOLD_TABLES)
    assert all(table["snapshot_id"] for table in first["gold_tables"].values())


def test_publication_rejects_missing_snapshot_metadata() -> None:
    with pytest.raises(ValueError, match="snapshot_id"):
        build_publication_document(
            source={},
            ingestion_run_id="run",
            identity_policy_version="policy",
            published_at="now",
            bronze={"row_count": 1, "snapshot_id": None},
            silver={"row_count": 1, "snapshot_id": "2"},
            quarantine={"row_count": 0, "snapshot_id": "3"},
            gold_tables=[
                TablePublication(name, f"s3://b/{name}", 1, "4")
                for name in REQUIRED_GOLD_TABLES
            ],
            dbt_summary={
                "status": "succeeded",
                "invocation_id": "dbt-invocation",
                "run_results_uri": "s3://bucket/dbt.json",
            },
        )


def test_monthly_retry_clear_and_rerun_compare_canonical_evidence() -> None:
    evidence = {
        "source_uri": "s3://bucket/month.parquet",
        "source_checksum": "a" * 64,
        "source_size_bytes": 10,
        "source_year": 2024,
        "source_month": 1,
        "ingestion_run_id": "stable-run",
        "identity_policy_version": "nyc-hvfhv-row-v1-2024",
        "bronze_row_count": 5,
        "silver_row_count": 1,
        "quarantine_row_count": 4,
        "gold_row_count": 1,
        "row_ids": ["row-a"],
        "quarantine_by_reason": {
            "DUPLICATE_ROW_ID": 1,
            "DROPOFF_BEFORE_PICKUP": 1,
            "UNKNOWN_PICKUP_ZONE": 1,
            "NEGATIVE_DRIVER_PAY": 1,
        },
    }
    compare_monthly_evidence(evidence, dict(evidence))
    with pytest.raises(ValueError, match="differences"):
        compare_monthly_evidence(evidence, {**evidence, "gold_row_count": 2})


def test_independent_reconciliation_and_schema_evolution_evidence() -> None:
    reconcile(
        {
            "source_row_count": 5,
            "bronze_row_count": 5,
            "silver_row_count": 1,
            "quarantine_row_count": 4,
            "quarantine_by_reason": {"DUPLICATE_ROW_ID": 4},
            "gold_row_count": 1,
            "publication_gold_row_count": 1,
            "athena_smoke_row_count": 1,
            "validation_status": "passed",
        }
    )
    verify(
        {
            "snapshot_2024": "101",
            "snapshot_2025": "202",
            "historical_2024_count": 1,
            "current_total_count": 2,
            "cbd_congestion_fee_nullable": True,
        }
    )
