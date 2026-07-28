"""Static Phase C deployment, packaging, authentication, and teardown contracts."""

from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

from scripts.package_spark_jobs import build

ROOT = Path(__file__).parents[2]


def test_spark_package_is_deterministic_and_contains_runtime_contract(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    manifest_one = build(first)
    manifest_two = build(second)
    assert manifest_one["entrypoints"] == manifest_two["entrypoints"]
    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        manifest = json.loads(archive.read("spark_runtime_manifest.json"))
        assert "etl/iceberg/catalog.py" in archive.namelist()
        assert "etl/spark_jobs/nyc_bronze_ingestion.py" in archive.namelist()
        assert manifest["namespaces"] == ["bronze", "silver", "ops"]


def test_terraform_is_nyc_only_and_uses_mwaa_and_package_contract() -> None:
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "terraform").glob("*.tf")
    )
    assert "instacart" not in terraform.lower()
    assert 'resource "aws_mwaa_environment"' in terraform
    assert '"PRIVATE_ONLY"' in terraform
    assert "aws_iam_instance_profile" not in terraform
    assert 'resource "aws_instance"' not in terraform
    assert "aws_emrserverless_application" in terraform
    assert "--py-files" in terraform
    assert 'aws_glue_catalog_database" "namespace' in terraform
    assert terraform.count('resource "aws_redshiftserverless_namespace"') == 1
    assert terraform.count('resource "aws_redshiftserverless_workgroup"') == 1
    assert terraform.count('resource "aws_iam_role" "redshift_spectrum"') == 1
    assert "manage_admin_password = true" in terraform
    assert "default_iam_role_arn" in terraform
    assert "warehouse_prefix}/bronze/*" in terraform
    assert "warehouse_prefix}/silver/*" in terraform
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze_external" in terraform
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS silver_external" in terraform
    assert "CREATE SCHEMA IF NOT EXISTS gold" in terraform
    assert "nyc_great_expectations_checkpoint.py" not in terraform
    assert "nyc_quality_checkpoint.py" not in terraform
    assert "nyc_publish_manifest.py" not in terraform


def test_teardown_has_no_apply() -> None:
    teardown = (ROOT / "scripts" / "teardown.ps1").read_text(encoding="utf-8").lower()
    assert "'plan', '-destroy'" in teardown
    assert "terraform apply" not in teardown


def test_cloud_environment_has_no_static_key_contract() -> None:
    env = (ROOT / ".env.cloud.example").read_text(encoding="utf-8")
    assert "AWS_ACCESS_KEY_ID" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "ATHENA_WORKGROUP" in env
    assert "AIRFLOW_VAR_NYC_EMR_SERVERLESS_APPLICATION_ID" in env
    assert "AIRFLOW_VAR_REDSHIFT_WORKGROUP_NAME" in env


def test_airflow_runtime_uses_one_plain_dbt_build_without_cosmos() -> None:
    requirements = (ROOT / "requirements-airflow.txt").read_text(encoding="utf-8")
    assert "dbt-redshift==1.10.2" in requirements
    assert "cosmos" not in requirements.lower()
