"""Static safety contract for the four approved Athena statements."""

from __future__ import annotations

from pathlib import Path
import re


SQL_DIR = Path(__file__).parents[2] / "athena" / "sql"
EXPECTED = {
    "gold_smoke.sql",
    "mart_hourly_zone_demand.sql",
    "iceberg_history.sql",
    "time_travel.sql.tmpl",
}
MUTATIONS = re.compile(
    r"\b(CTAS|UNLOAD|INSERT|UPDATE|DELETE|MERGE|OPTIMIZE|VACUUM|CREATE|DROP|ALTER)\b",
    re.IGNORECASE,
)


def test_exactly_four_bounded_sql_artifacts_exist() -> None:
    assert {
        path.name for path in SQL_DIR.iterdir() if path.suffix in {".sql", ".tmpl"}
    } == EXPECTED


def test_queries_are_single_read_only_gold_statements_with_limits() -> None:
    for path in SQL_DIR.iterdir():
        if not path.is_file() or path.name == "__init__.py":
            continue
        sql = path.read_text(encoding="utf-8")
        body = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        ).strip()
        assert not MUTATIONS.search(body), path.name
        assert body.count(";") == 1, path.name
        assert '"gold".' in body or "{{gold_database}}" in body, path.name
        if path.name != "gold_smoke.sql":
            assert re.search(r"\bLIMIT\s+\d+", body, re.IGNORECASE), path.name


def test_time_travel_template_has_only_identifier_placeholders_and_bound_snapshot() -> (
    None
):
    template = (SQL_DIR / "time_travel.sql.tmpl").read_text(encoding="utf-8")
    assert set(re.findall(r"{{([^}]+)}}", template)) == {"gold_database", "gold_table"}
    assert "FOR VERSION AS OF ?" in template
