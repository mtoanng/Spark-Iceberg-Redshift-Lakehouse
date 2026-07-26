"""Deterministic publication document construction without AWS calls."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Mapping, Sequence


MANIFEST_VERSION = "nyc-hvfhv-publication-v1"
REQUIRED_GOLD_TABLES = (
    "dim_date",
    "dim_operator",
    "dim_zone",
    "fct_trips",
    "mart_hourly_zone_demand",
    "mart_operator_metrics",
)


@dataclass(frozen=True)
class TablePublication:
    name: str
    location: str
    row_count: int
    snapshot_id: str


def publication_key(year: int, month: int, run_id: str) -> str:
    return f"year={year}/month={month:02d}/{run_id}.json"


def build_publication_document(
    *,
    source: Mapping[str, object],
    ingestion_run_id: str,
    identity_policy_version: str,
    published_at: str,
    bronze: Mapping[str, object],
    silver: Mapping[str, object],
    quarantine: Mapping[str, object],
    gold_tables: Sequence[TablePublication],
    dbt_summary: Mapping[str, object],
) -> dict[str, object]:
    table_map = {table.name: table for table in gold_tables}
    missing = set(REQUIRED_GOLD_TABLES) - set(table_map)
    if missing:
        raise ValueError(
            "Publication missing Gold tables: " + ", ".join(sorted(missing))
        )
    for layer_name, layer in (
        ("bronze", bronze),
        ("silver", silver),
        ("quarantine", quarantine),
    ):
        if int(layer.get("row_count", -1)) < 0 or not layer.get("snapshot_id"):
            raise ValueError(
                f"Publication requires {layer_name} count and snapshot_id."
            )
    for table in table_map.values():
        if table.row_count < 0 or not table.location or not table.snapshot_id:
            raise ValueError(f"Incomplete publication metadata for {table.name}.")
    if (
        dbt_summary.get("status") != "succeeded"
        or not dbt_summary.get("invocation_id")
        or not dbt_summary.get("run_results_uri")
    ):
        raise ValueError(
            "Publication requires retained successful dbt invocation metadata."
        )
    return {
        "manifest_version": MANIFEST_VERSION,
        "source": dict(source),
        "ingestion_run_id": ingestion_run_id,
        "identity_policy_version": identity_policy_version,
        "published_at": published_at,
        "validation_status": "passed",
        "bronze": dict(bronze),
        "silver": dict(silver),
        "quarantine": dict(quarantine),
        "gold_tables": {name: asdict(table_map[name]) for name in sorted(table_map)},
        "dbt": dict(dbt_summary),
    }


def canonical_json(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
