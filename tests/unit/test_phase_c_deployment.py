"""Static Phase C deployment, packaging, authentication, and teardown contracts."""

from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

from scripts.package_glue_jobs import build
from scripts.run_e2e import command_plan


ROOT = Path(__file__).parents[2]


def test_glue_package_is_deterministic_and_contains_runtime_contract(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    manifest_one = build(first)
    manifest_two = build(second)
    assert manifest_one["entrypoints"] == manifest_two["entrypoints"]
    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        manifest = json.loads(archive.read("glue_runtime_manifest.json"))
        assert "etl/iceberg/catalog.py" in archive.namelist()
        assert manifest["namespaces"] == ["bronze", "silver", "ops", "gold"]


def test_terraform_is_nyc_only_and_uses_profile_and_package_contract() -> None:
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "terraform").glob("*.tf")
    )
    assert "instacart" not in terraform.lower()
    assert "aws_iam_instance_profile" in terraform
    assert 'http_tokens                 = "required"' in terraform
    assert "--extra-py-files" in terraform
    assert 'aws_glue_catalog_database" "namespace' in terraform


def test_e2e_release_is_four_months_and_teardown_has_no_apply() -> None:
    assert len(command_plan(2024, 1)) == 4
    teardown = (ROOT / "scripts" / "teardown.ps1").read_text(encoding="utf-8").lower()
    assert "terraform plan -destroy" in teardown
    assert "terraform apply" not in teardown


def test_cloud_environment_has_no_static_key_contract() -> None:
    env = (ROOT / ".env.cloud.example").read_text(encoding="utf-8")
    assert "AWS_ACCESS_KEY_ID" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "ATHENA_WORKGROUP" in env
    assert "AIRFLOW_VAR_NYC_GREAT_EXPECTATIONS_JOB_NAME" in env
