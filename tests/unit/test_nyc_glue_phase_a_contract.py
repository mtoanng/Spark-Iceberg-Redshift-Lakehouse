"""Static contracts for remote-only Phase A Glue entry points."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[2] / "etl" / "glue_jobs"


def _job(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def test_bronze_is_month_scoped_and_uses_manifest_guarded_partition_replacement() -> (
    None
):
    source = _job("nyc_bronze_ingestion.py")
    assert "MANIFEST_TABLE" in source
    assert "Changed source URI, checksum, or size is blocked" in source
    assert source.count(".overwritePartitions()") == 2
    assert (
        "SOURCE_YEAR" in source
        and "SOURCE_MONTH" in source
        and "INGESTION_RUN_ID" in source
    )
    assert "validate_landed_source" in source
    assert "SOURCE_SIZE_BYTES" in source
    assert "stable_run_id(source)" in source
    assert "StorageLevel.MEMORY_AND_DISK" in source
    assert "trips.unpersist()" in source
    assert "target.source_size_bytes <> source.source_size_bytes" in source
    assert "Changed source URI, checksum, or size was rejected." in source
    assert "publication_manifest_uri = CASE" in source
    assert (
        "target.run_status IN ('silver_published', 'reconciled', 'published')" in source
    )
    assert 'boto3.client("s3").head_object' in source
    assert "actual_checksum != expected_checksum" in source
    assert "ContentLength" in source


def test_ge_checkpoint_runs_before_silver_contract_and_persists_result() -> None:
    source = _job("nyc_great_expectations_checkpoint.py")
    assert "import great_expectations as gx" in source
    assert "required_identity_columns" in source
    assert 'manifest.run_status != "bronze_published"' in source
    assert "batch.validate(expectation_suite=_suite(year))" in source
    assert "validation_result_summary" in source
    assert (
        "ge_blocked" in source
        and "Great Expectations blocking checkpoint failed" in source
    )
    assert "observed_invalid_row_count" not in source
    assert "BRONZE_ZONES_TABLE" not in source


def test_silver_filters_one_run_requires_ge_and_overwrites_month_partition() -> None:
    source = _job("nyc_silver_transform.py")
    assert 'run_status != "ge_passed"' in source
    assert 'col("_ingestion_run_id") == run_id' in source
    assert source.count(".overwritePartitions()") == 2
    assert "Bronze/Silver/quarantine reconciliation failed" in source
    assert "StorageLevel.MEMORY_AND_DISK" in source
    assert "retrying_silver" in source
    assert 'failure_stage == "silver"' in source
    assert "spark_reason_expression" in source


def test_reconciliation_uses_correct_partition_columns_and_updates_manifest() -> None:
    source = _job("nyc_quality_checkpoint.py")
    assert 'manifest.run_status != "silver_published"' in source
    assert "bronze_vs_classified" in source
    assert "gold_vs_silver" in source
    assert "run_status='reconciled'" in source
    assert "gold_row_count" in source
    assert "publication_status='pending'" in source


def test_publication_requires_reconciliation_and_writes_six_gold_tables() -> None:
    source = _job("nyc_publish_manifest.py")
    assert 'row.run_status != "reconciled"' in source
    assert 'boto3.client("s3").put_object' in source
    assert "snapshot_id" in source
    assert "publication_status" in source
    assert "REQUIRED_GOLD_TABLES" in source
    contract = (ROOT.parent / "publication" / "nyc_hvfhs.py").read_text(
        encoding="utf-8"
    )
    for table in (
        "dim_date",
        "dim_operator",
        "dim_zone",
        "fct_trips",
        "mart_hourly_zone_demand",
        "mart_operator_metrics",
    ):
        assert table in contract
