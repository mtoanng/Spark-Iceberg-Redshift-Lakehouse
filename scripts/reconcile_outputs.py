"""Credential-independent reconciliation for a retained manifest JSON object."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def reconcile(manifest: dict[str, object]) -> None:
    source = int(manifest.get("source_row_count", manifest["bronze_row_count"]))
    bronze = int(manifest["bronze_row_count"])
    silver = int(manifest["silver_row_count"])
    quarantine_by_reason = manifest.get("quarantine_by_reason", {})
    quarantine = int(
        manifest.get(
            "quarantine_row_count",
            sum(int(value) for value in quarantine_by_reason.values()),
        )
    )
    gold = int(manifest.get("gold_row_count", silver))
    publication = int(manifest.get("publication_gold_row_count", gold))
    athena = int(manifest.get("athena_smoke_row_count", publication))
    differences = {
        "source_vs_bronze": source - bronze,
        "bronze_vs_classified": bronze - silver - quarantine,
        "quarantine_reason_total": quarantine
        - sum(int(value) for value in quarantine_by_reason.values()),
        "gold_vs_silver": gold - silver,
        "publication_vs_gold": publication - gold,
        "athena_vs_publication": athena - publication,
    }
    if any(differences.values()) or manifest.get("validation_status") not in {
        "validated",
        "passed",
    }:
        raise ValueError(f"publication reconciliation differences: {differences}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    reconcile(json.loads(args.manifest.read_text(encoding="utf-8")))
    print(f"PASS reconciled={args.manifest}")


if __name__ == "__main__":
    main()
