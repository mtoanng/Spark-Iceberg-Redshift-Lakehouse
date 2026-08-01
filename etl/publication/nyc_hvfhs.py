"""Deterministic, Redshift-aware publication document construction."""

from __future__ import annotations

import json
from typing import Mapping


MANIFEST_VERSION = "nyc-hvfhv-publication-v2"
REQUIRED_GOLD_RELATIONS = (
    "dim_date",
    "dim_operator",
    "dim_zone",
    "fct_trips",
    "mart_hourly_zone_demand",
    "mart_operator_metrics",
)


def publication_key(year: int, month: int, run_id: str) -> str:
    return f"year={year}/month={month:02d}/{run_id}.json"


def build_publication_document(
    *,
    source: Mapping[str, object],
    ingestion_run_id: str,
    identity_policy_version: str,
    iceberg_layers: Mapping[str, Mapping[str, object]],
    redshift_database: str,
    redshift_schema: str,
    reconciliation: Mapping[str, object],
    dbt_artifact_uri: str,
    dbt_artifact_sha256: str,
    published_at: str,
) -> dict[str, object]:
    """Build one complete logical publication document without AWS calls."""

    required_source = {
        "source_uri",
        "source_checksum",
        "source_size_bytes",
        "source_year",
        "source_month",
    }
    if (
        required_source - set(source)
        or not ingestion_run_id
        or not identity_policy_version
    ):
        raise ValueError("Publication requires immutable source and identity metadata.")
    if set(iceberg_layers) != {"bronze", "silver", "quarantine"}:
        raise ValueError("Publication requires Bronze, Silver, and quarantine layers.")
    for name, layer in iceberg_layers.items():
        if not layer.get("table_identifier"):
            raise ValueError(f"Publication requires a {name} table identifier.")
        if not layer.get("snapshot_id"):
            raise ValueError(f"Publication requires a {name} snapshot ID.")
        if int(layer.get("row_count", -1)) < 0:
            raise ValueError(f"Publication requires a non-negative {name} row count.")
    if not redshift_database or not redshift_schema:
        raise ValueError("Publication requires the Redshift database and schema.")
    if not dbt_artifact_uri or len(dbt_artifact_sha256) != 64:
        raise ValueError("Publication requires the retained dbt artifact and SHA-256.")
    if not reconciliation.get("bronze_equals_classified") or not reconciliation.get(
        "silver_equals_gold"
    ):
        raise ValueError("Publication requires successful reconciliation invariants.")
    return {
        "manifest_version": MANIFEST_VERSION,
        "status": "published",
        "source": dict(source),
        "ingestion_run_id": ingestion_run_id,
        "identity_policy_version": identity_policy_version,
        "iceberg_layers": {
            name: dict(iceberg_layers[name]) for name in sorted(iceberg_layers)
        },
        "redshift": {
            "database": redshift_database,
            "schema": redshift_schema,
            "gold_relations": list(REQUIRED_GOLD_RELATIONS),
        },
        "row_counts": {
            "bronze": int(reconciliation["bronze_row_count"]),
            "silver": int(reconciliation["silver_row_count"]),
            "quarantine": int(reconciliation["quarantine_row_count"]),
            "gold_fct_trips": int(reconciliation["gold_row_count"]),
        },
        "reconciliation": dict(reconciliation),
        "dbt": {
            "run_results_uri": dbt_artifact_uri,
            "sha256": dbt_artifact_sha256,
        },
        "publication_timestamp": published_at,
    }


def canonical_json(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def logical_document(document: Mapping[str, object]) -> dict[str, object]:
    """Remove attempt metadata so identical source reruns reuse one release."""

    result = dict(document)
    result.pop("publication_timestamp", None)
    reconciliation = dict(result.get("reconciliation", {}))
    for field in (
        "redshift_statement_id",
        "reconciled_at",
    ):
        reconciliation.pop(field, None)
    result["reconciliation"] = reconciliation
    return result
