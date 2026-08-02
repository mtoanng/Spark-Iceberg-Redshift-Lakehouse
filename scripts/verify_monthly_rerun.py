"""Compare retained monthly evidence for retry/clear/rerun safety."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping


STABLE_FIELDS = (
    "source_uri",
    "source_checksum",
    "source_size_bytes",
    "source_year",
    "source_month",
    "ingestion_run_id",
    "identity_policy_version",
    "bronze_row_count",
    "silver_row_count",
    "quarantine_row_count",
    "gold_row_count",
)


def compare_monthly_evidence(
    first: Mapping[str, object], rerun: Mapping[str, object]
) -> None:
    differences = {
        field: (first.get(field), rerun.get(field))
        for field in STABLE_FIELDS
        if first.get(field) != rerun.get(field)
    }
    first_ids, rerun_ids = set(first.get("row_ids", [])), set(rerun.get("row_ids", []))
    if first_ids != rerun_ids:
        differences["row_ids"] = (sorted(first_ids), sorted(rerun_ids))
    first_reasons = first.get("quarantine_by_reason", {})
    rerun_reasons = rerun.get("quarantine_by_reason", {})
    if first_reasons != rerun_reasons:
        differences["quarantine_by_reason"] = (first_reasons, rerun_reasons)
    if differences:
        raise ValueError(f"Deterministic monthly rerun differences: {differences}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("first", type=Path)
    parser.add_argument("rerun", type=Path)
    args = parser.parse_args()
    compare_monthly_evidence(
        json.loads(args.first.read_text(encoding="utf-8")),
        json.loads(args.rerun.read_text(encoding="utf-8")),
    )
    print("PASS monthly retry/clear/rerun evidence is identical")


if __name__ == "__main__":
    main()
