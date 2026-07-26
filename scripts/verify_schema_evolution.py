"""Verify retained evidence for the single approved 2025 evolution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def verify(evidence: dict[str, object]) -> None:
    old_snapshot = str(evidence.get("snapshot_2024", ""))
    current_snapshot = str(evidence.get("snapshot_2025", ""))
    old_count = int(evidence.get("historical_2024_count", -1))
    current_count = int(evidence.get("current_total_count", -1))
    if (
        not old_snapshot
        or not current_snapshot
        or old_snapshot == current_snapshot
        or old_count < 1
        or current_count <= old_count
        or not evidence.get("cbd_congestion_fee_nullable")
    ):
        raise ValueError("Schema-evolution/version-travel evidence is incomplete.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    verify(json.loads(args.evidence.read_text(encoding="utf-8")))
    print("PASS 2024 snapshot and 2025 current snapshot evidence reconcile")


if __name__ == "__main__":
    main()
