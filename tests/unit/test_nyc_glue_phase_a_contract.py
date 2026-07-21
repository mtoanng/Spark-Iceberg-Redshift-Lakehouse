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
    assert "Changed source URI or checksum is blocked" in source
    assert source.count(".overwritePartitions()") == 2
    assert (
        "SOURCE_YEAR" in source
        and "SOURCE_MONTH" in source
        and "INGESTION_RUN_ID" in source
    )


def test_ge_checkpoint_runs_before_silver_contract_and_persists_result() -> None:
    source = _job("nyc_great_expectations_checkpoint.py")
    assert "import great_expectations as gx" in source
    assert "batch.validate(expectation_suite=_suite())" in source
    assert "validation_result_summary" in source
    assert (
        "ge_blocked" in source
        and "Great Expectations blocking checkpoint failed" in source
    )


def test_silver_filters_one_run_requires_ge_and_overwrites_month_partition() -> None:
    source = _job("nyc_silver_transform.py")
    assert 'run_status != "ge_passed"' in source
    assert 'col("_ingestion_run_id") == run_id' in source
    assert source.count(".overwritePartitions()") == 2
    assert "Bronze/Silver/quarantine reconciliation failed" in source
