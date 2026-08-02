"""Deterministic publication, rerun, reconciliation, and evolution contracts."""

from __future__ import annotations

import pytest

from etl.publication.nyc_hvfhs import (
    build_publication_document,
    canonical_json,
)
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
        iceberg_layers={
            "bronze": {
                "table_identifier": "bronze.bronze_hvfhs_trips",
                "row_count": 5,
                "snapshot_id": "101",
            },
            "silver": {
                "table_identifier": "silver.silver_trips",
                "row_count": 1,
                "snapshot_id": "102",
            },
            "quarantine": {
                "table_identifier": "silver.quarantine_trips",
                "row_count": 4,
                "snapshot_id": "103",
            },
        },
        redshift_database="lakehouse",
        redshift_schema="gold",
        reconciliation={
            "bronze_row_count": 5,
            "silver_row_count": 1,
            "quarantine_row_count": 4,
            "gold_row_count": 1,
            "bronze_equals_classified": True,
            "silver_equals_gold": True,
        },
        dbt_artifact_uri="s3://bucket/manifests/dbt-results.json",
        dbt_artifact_sha256="a" * 64,
    )


def test_publication_is_complete_and_byte_deterministic() -> None:
    first = _publication()
    second = _publication()
    assert canonical_json(first) == canonical_json(second)
    assert first["redshift"]["schema"] == "gold"
    assert len(first["redshift"]["gold_relations"]) == 6
    assert "snapshot_id" not in first["redshift"]


def test_publication_rejects_missing_snapshot_metadata() -> None:
    with pytest.raises(ValueError, match="immutable source"):
        build_publication_document(
            source={},
            ingestion_run_id="run",
            identity_policy_version="policy",
            published_at="now",
            iceberg_layers={},
            redshift_database="lakehouse",
            redshift_schema="gold",
            reconciliation={},
            dbt_artifact_uri="s3://bucket/dbt.json",
            dbt_artifact_sha256="a" * 64,
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


def test_schema_evolution_evidence_contract() -> None:
    verify(
        {
            "snapshot_2024": "101",
            "snapshot_2025": "202",
            "historical_2024_count": 1,
            "current_total_count": 2,
            "cbd_congestion_fee_nullable": True,
        }
    )
