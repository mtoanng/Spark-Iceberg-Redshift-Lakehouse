from datetime import datetime, timedelta, timezone

import pytest

from etl.iceberg.lifecycle import (
    GOLD_TABLES,
    LifecycleContractError,
    RetentionPolicy,
    build_snapshot_manifest,
    orphan_file_dry_run,
    pinned_snapshot_reference,
    plan_2025_hvfhs_schema_evolution,
    retention_dry_run,
    should_compact,
)


def _snapshot_ids() -> dict[str, int]:
    return {table: index + 100 for index, table in enumerate(sorted(GOLD_TABLES))}


def test_2025_schema_evolution_plan_is_nullable_and_bounded():
    plan = plan_2025_hvfhs_schema_evolution()

    assert plan.table == "glue_catalog.bronze.bronze_hvfhs_trips"
    assert plan.added_columns == (("cbd_congestion_fee", "DECIMAL(18,2)"),)
    assert plan.ddl.endswith("ADD COLUMNS (cbd_congestion_fee DECIMAL(18,2))")
    with pytest.raises(LifecycleContractError, match="2025"):
        plan_2025_hvfhs_schema_evolution(source_year=2024)


def test_snapshot_manifest_requires_all_gold_tables_and_is_stable():
    captured = datetime(2026, 7, 19, tzinfo=timezone.utc)
    manifest = build_snapshot_manifest(_snapshot_ids(), source_year=2025, source_month=1, captured_at=captured)

    assert pinned_snapshot_reference(manifest, "fct_trips").snapshot_id == _snapshot_ids()["fct_trips"]
    assert '"source_month":1' in manifest.to_json()
    with pytest.raises(LifecycleContractError, match="exactly the six"):
        build_snapshot_manifest({"fct_trips": 1}, source_year=2025, source_month=1)


def test_compaction_requires_small_file_pressure():
    assert should_compact(101, 10) is True
    assert should_compact(101, 32) is False
    assert should_compact(100, 10) is False


def test_retention_is_a_dry_run_and_keeps_recent_snapshots():
    now = datetime(2026, 7, 19, tzinfo=timezone.utc)
    snapshots = [(1, now - timedelta(days=30)), (2, now - timedelta(days=20)), (3, now - timedelta(days=10)), (4, now - timedelta(days=1))]

    assert retention_dry_run(snapshots, now=now, policy=RetentionPolicy(minimum_snapshots=2, minimum_age_days=7)) == (2, 1)
    with pytest.raises(LifecycleContractError, match="dry run"):
        RetentionPolicy(dry_run=False)


def test_orphan_file_operation_only_lists_candidates():
    assert orphan_file_dry_run(["a/data.parquet"], ["a/data.parquet", "a/orphan.parquet"]) == ("a/orphan.parquet",)
