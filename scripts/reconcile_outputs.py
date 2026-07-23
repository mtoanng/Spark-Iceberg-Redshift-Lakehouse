"""Credential-independent reconciliation for a retained manifest JSON object."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def reconcile(manifest: dict[str, object]) -> None:
    bronze = int(manifest["bronze_row_count"])
    silver = int(manifest["silver_row_count"])
    quarantine = int(manifest["quarantine_row_count"])
    gold = int(manifest.get("gold_row_count", silver))
    if (
        bronze != silver + quarantine
        or gold != silver
        or manifest.get("validation_status") not in {"validated", "passed"}
    ):
        raise ValueError("publication manifest reconciliation failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    reconcile(json.loads(args.manifest.read_text(encoding="utf-8")))
    print(f"PASS reconciled={args.manifest}")


if __name__ == "__main__":
    main()
