"""Credential-free planning contracts for the optional Iceberg lifecycle work.

These functions plan safe operations and validate manifests. They do not call
AWS, delete files, expire snapshots, or claim that a chosen catalog supports a
particular DuckDB snapshot syntax.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Iterable, Mapping


class LifecycleContractError(ValueError):
    """Raised when a lifecycle plan could be unsafe or incomplete."""


BRONZE_HVFH_TABLE = "glue_catalog.bronze.bronze_hvfhs_trips"
GOLD_TABLES = frozenset(
    {
        "dim_date",
        "dim_operator",
        "dim_zone",
        "fct_trips",
        "mart_hourly_zone_demand",
        "mart_operator_metrics",
    }
)
_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


@dataclass(frozen=True)
class SchemaEvolutionPlan:
    table: str
    source_year: int
    source_month: int
    added_columns: tuple[tuple[str, str], ...]
    ddl: str


def plan_2025_hvfhs_schema_evolution(
    *, table: str = BRONZE_HVFH_TABLE, source_year: int = 2025, source_month: int = 1
) -> SchemaEvolutionPlan:
    """Plan the nullable 2025 congestion-fee addition without executing it."""

    if table != BRONZE_HVFH_TABLE or not _IDENTIFIER.fullmatch(table):
        raise LifecycleContractError("schema evolution is restricted to the Bronze HVFHV table")
    if source_year < 2025 or not 1 <= source_month <= 12:
        raise LifecycleContractError("the evolution plan requires a 2025-or-later month")
    columns = (("cbd_congestion_fee", "DECIMAL(18,2)"),)
    return SchemaEvolutionPlan(
        table=table,
        source_year=source_year,
        source_month=source_month,
        added_columns=columns,
        ddl=f"ALTER TABLE {table} ADD COLUMNS (cbd_congestion_fee DECIMAL(18,2))",
    )


@dataclass(frozen=True)
class SnapshotReference:
    table: str
    snapshot_id: int

    def __post_init__(self) -> None:
        if self.table not in GOLD_TABLES or self.snapshot_id <= 0:
            raise LifecycleContractError("snapshot references require a Gold table and positive snapshot ID")


@dataclass(frozen=True)
class SnapshotManifest:
    source_year: int
    source_month: int
    captured_at: datetime
    snapshots: tuple[SnapshotReference, ...]

    def __post_init__(self) -> None:
        names = {item.table for item in self.snapshots}
        if names != GOLD_TABLES or len(self.snapshots) != len(GOLD_TABLES):
            raise LifecycleContractError("a Gold snapshot manifest must contain each of the six tables exactly once")

    def to_json(self) -> str:
        payload = {
            "source_year": self.source_year,
            "source_month": self.source_month,
            "captured_at": self.captured_at.astimezone(timezone.utc).isoformat(),
            "snapshots": [
                {"table": item.table, "snapshot_id": item.snapshot_id}
                for item in sorted(self.snapshots, key=lambda item: item.table)
            ],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def build_snapshot_manifest(
    snapshot_ids: Mapping[str, int],
    *,
    source_year: int,
    source_month: int,
    captured_at: datetime | None = None,
) -> SnapshotManifest:
    if set(snapshot_ids) != GOLD_TABLES:
        raise LifecycleContractError("snapshot IDs must cover exactly the six Gold tables")
    return SnapshotManifest(
        source_year=source_year,
        source_month=source_month,
        captured_at=captured_at or datetime.now(timezone.utc),
        snapshots=tuple(SnapshotReference(table, int(snapshot_ids[table])) for table in sorted(GOLD_TABLES)),
    )


def pinned_snapshot_reference(manifest: SnapshotManifest, table: str) -> SnapshotReference:
    """Return an explicit ID for a connector-specific pinned query adapter."""

    for reference in manifest.snapshots:
        if reference.table == table:
            return reference
    raise LifecycleContractError(f"table is not present in snapshot manifest: {table}")


@dataclass(frozen=True)
class CompactionPolicy:
    max_data_files: int = 100
    min_average_file_size_mb: int = 32


def should_compact(file_count: int, average_file_size_mb: float, policy: CompactionPolicy = CompactionPolicy()) -> bool:
    """Plan compaction only when both small-file thresholds are exceeded."""

    if file_count < 0 or average_file_size_mb < 0:
        raise LifecycleContractError("file metrics cannot be negative")
    return file_count > policy.max_data_files and average_file_size_mb < policy.min_average_file_size_mb


@dataclass(frozen=True)
class RetentionPolicy:
    minimum_snapshots: int = 2
    minimum_age_days: int = 7
    dry_run: bool = True

    def __post_init__(self) -> None:
        if self.minimum_snapshots < 1 or self.minimum_age_days < 0:
            raise LifecycleContractError("retention thresholds must be non-negative and keep at least one snapshot")
        if not self.dry_run:
            raise LifecycleContractError("retention execution is disabled; produce a dry run first")


def retention_dry_run(
    snapshot_times: Iterable[tuple[int, datetime]],
    *,
    now: datetime,
    policy: RetentionPolicy = RetentionPolicy(),
) -> tuple[int, ...]:
    """Return old snapshot IDs eligible for review, never for automatic deletion."""

    snapshots = sorted(snapshot_times, key=lambda item: item[1], reverse=True)
    cutoff = now - timedelta(days=policy.minimum_age_days)
    return tuple(snapshot_id for index, (snapshot_id, committed_at) in enumerate(snapshots) if index >= policy.minimum_snapshots and committed_at < cutoff)


def orphan_file_dry_run(referenced_paths: Iterable[str], discovered_paths: Iterable[str]) -> tuple[str, ...]:
    """List unreferenced files for review; this function never deletes anything."""

    referenced = set(referenced_paths)
    return tuple(sorted(set(discovered_paths) - referenced))
