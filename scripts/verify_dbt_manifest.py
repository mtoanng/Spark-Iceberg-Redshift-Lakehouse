"""Verify the exact dbt graph produced by the single Gold build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


EXPECTED_DEPENDENCIES = {
    "dim_date": set(),
    "dim_operator": set(),
    "dim_zone": set(),
    "fct_trips": set(),
    "mart_hourly_zone_demand": {"fct_trips"},
    "mart_operator_metrics": {"fct_trips"},
}
EXPECTED_TEST_COUNT = 37


def verify(manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    nodes = manifest["nodes"]
    models = {
        node["name"]: node
        for node in nodes.values()
        if node.get("resource_type") == "model"
        and node.get("package_name") == "nyc_hvfhs_lakehouse"
    }
    tests = [
        node
        for node in nodes.values()
        if node.get("resource_type") == "test"
        and node.get("package_name") == "nyc_hvfhs_lakehouse"
    ]
    if set(models) != set(EXPECTED_DEPENDENCIES):
        raise ValueError(f"Unexpected dbt models: {sorted(models)}")
    if len(tests) != EXPECTED_TEST_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_TEST_COUNT} dbt tests, found {len(tests)}"
        )

    graph = {}
    for name, node in models.items():
        graph[name] = {
            nodes[node_id]["name"]
            for node_id in node.get("depends_on", {}).get("nodes", [])
            if node_id in nodes and nodes[node_id].get("resource_type") == "model"
        }
    if graph != EXPECTED_DEPENDENCIES:
        raise ValueError(f"Unexpected dbt dependency graph: {graph}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("etl/dbt_project/target/manifest.json"),
    )
    args = parser.parse_args()
    verify(args.manifest)
    print("PASS dbt manifest: 6 models, 37 tests, dependency graph unchanged")


if __name__ == "__main__":
    main()
