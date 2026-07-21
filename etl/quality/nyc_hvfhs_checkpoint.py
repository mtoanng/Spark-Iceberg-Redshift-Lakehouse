"""Pure-Python equivalent of the Phase 5 NYC quality checkpoint."""

from __future__ import annotations

from dataclasses import dataclass

from etl.transforms.nyc_hvfhs import BronzeBatch, SilverBatch, reconcile


class QualityCheckpointError(ValueError):
    """Raised when a batch cannot be promoted past the explicit quality gate."""


@dataclass(frozen=True)
class QualityCheckpointResult:
    bronze_count: int
    silver_count: int
    quarantine_count: int
    distinct_trip_count: int


def evaluate_fixture_checkpoint(bronze: BronzeBatch, silver: SilverBatch) -> QualityCheckpointResult:
    """Validate reconciliation, quarantine evidence, and canonical trip uniqueness."""

    reconciliation = reconcile(bronze, silver)
    missing_reason = [row for row in silver.quarantine_rows if not row.get("reason_code")]
    if missing_reason:
        raise QualityCheckpointError("Quarantine rows must include a reason_code.")

    trip_ids = [str(row.get("trip_id", "")) for row in silver.silver_rows]
    if not all(trip_ids):
        raise QualityCheckpointError("Silver rows must include trip_id.")
    if len(set(trip_ids)) != len(trip_ids):
        raise QualityCheckpointError("Silver trip_id values must be unique.")

    return QualityCheckpointResult(
        bronze_count=reconciliation.bronze_count,
        silver_count=reconciliation.silver_count,
        quarantine_count=reconciliation.quarantine_count,
        distinct_trip_count=len(set(trip_ids)),
    )
