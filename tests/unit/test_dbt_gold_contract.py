"""Static, credential-independent checks for the locked Phase 3 dbt graph."""

from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).parents[2]
MODEL_ROOT = PROJECT_ROOT / "etl" / "dbt_project" / "models_nyc"
EXPECTED_MODELS = {
    "dim_operator",
    "dim_zone",
    "dim_date",
    "fct_trips",
    "mart_hourly_zone_demand",
    "mart_operator_metrics",
}


def test_gold_contains_exactly_the_locked_six_models() -> None:
    actual = {path.stem for path in MODEL_ROOT.rglob("*.sql")}
    assert actual == EXPECTED_MODELS


def test_every_gold_model_is_redshift_managed_and_has_no_legacy_scope() -> None:
    for path in MODEL_ROOT.rglob("*.sql"):
        sql = path.read_text(encoding="utf-8").lower()
        assert "file_format" not in sql
        assert "iceberg" not in sql
        assert "partition_by" not in sql
        assert "date_format(" not in sql
        assert " as string)" not in sql
        assert "instacart" not in sql
        assert "recommend" not in sql
        assert "mongo" not in sql
        assert "ml" not in sql


def test_fact_and_mart_grains_are_declared() -> None:
    fact_sql = (MODEL_ROOT / "facts" / "fct_trips.sql").read_text(encoding="utf-8")
    hourly_sql = (MODEL_ROOT / "marts" / "mart_hourly_zone_demand.sql").read_text(
        encoding="utf-8"
    )
    operator_sql = (MODEL_ROOT / "marts" / "mart_operator_metrics.sql").read_text(
        encoding="utf-8"
    )
    assert "one row per validated, deduplicated Silver row_id" in fact_sql
    assert "pickup date, pickup hour, and pickup zone" in hourly_sql
    assert "source year, source month, and operator" in operator_sql
    assert "ref('fct_trips')" in hourly_sql
    assert "ref('fct_trips')" in operator_sql
    assert "materialized='incremental'" in fact_sql
    assert "incremental_strategy='merge'" in fact_sql
    assert "unique_key='row_id'" in fact_sql
    assert "var('source_year')" in fact_sql and "var('source_month')" in fact_sql


def test_schema_declares_tests_for_exactly_six_models() -> None:
    schema = yaml.safe_load((MODEL_ROOT / "schema.yml").read_text(encoding="utf-8"))
    assert {model["name"] for model in schema["models"]} == EXPECTED_MODELS
    fact = next(model for model in schema["models"] if model["name"] == "fct_trips")
    row_id = next(column for column in fact["columns"] if column["name"] == "row_id")
    assert set(row_id["data_tests"]) == {"unique", "not_null"}


def test_fact_to_silver_reconciliation_test_exists() -> None:
    test_sql = (
        PROJECT_ROOT
        / "etl"
        / "dbt_project"
        / "tests"
        / "fct_trips_reconciles_to_silver.sql"
    ).read_text(encoding="utf-8")
    assert "source('silver', 'silver_trips')" in test_sql
    assert "ref('fct_trips')" in test_sql
    assert "<>" in test_sql


def test_redshift_profile_uses_serverless_iam_and_no_glue_target() -> None:
    profile = (PROJECT_ROOT / "etl" / "dbt_project" / "profiles.yml").read_text(
        encoding="utf-8"
    )
    assert "target: redshift" in profile
    assert "type: redshift" in profile
    assert "method: iam_role" in profile
    assert "is_serverless: true" in profile
    assert "type: glue" not in profile
    assert "target: glue" not in profile

    workflow = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    assert "DBT_CI_REDSHIFT_PASSWORD" in workflow
    assert "dbt-redshift==1.10.2" in (PROJECT_ROOT / "requirements-ci.txt").read_text(
        encoding="utf-8"
    )
    for requirements in PROJECT_ROOT.glob("requirements*.txt"):
        assert "dbt" + "-glue" not in requirements.read_text(encoding="utf-8").lower()


def test_model_dependency_graph_is_unchanged() -> None:
    dependencies = {}
    for path in MODEL_ROOT.rglob("*.sql"):
        sql = path.read_text(encoding="utf-8")
        dependencies[path.stem] = {
            model
            for model in EXPECTED_MODELS
            if f"ref('{model}')" in sql or f'ref("{model}")' in sql
        }
    assert dependencies == {
        "dim_date": set(),
        "dim_operator": set(),
        "dim_zone": set(),
        "fct_trips": set(),
        "mart_hourly_zone_demand": {"fct_trips"},
        "mart_operator_metrics": {"fct_trips"},
    }


def test_sources_use_redshift_external_schemas() -> None:
    sources = yaml.safe_load((MODEL_ROOT / "sources.yml").read_text(encoding="utf-8"))
    schemas = {source["name"]: source["schema"] for source in sources["sources"]}
    assert schemas == {"bronze": "bronze_external", "silver": "silver_external"}
