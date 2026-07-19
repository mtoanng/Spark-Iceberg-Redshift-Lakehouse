"""Iceberg catalog/table contracts for the NYC HVFHV lakehouse."""

from .catalog import TABLE_SPECS, TableSpec, namespace_ddl, table_ddl
from .lifecycle import (
    CompactionPolicy,
    LifecycleContractError,
    RetentionPolicy,
    SnapshotManifest,
    SnapshotReference,
    build_snapshot_manifest,
    orphan_file_dry_run,
    plan_2025_hvfhs_schema_evolution,
    pinned_snapshot_reference,
    retention_dry_run,
    should_compact,
)

__all__ = [
    "TABLE_SPECS",
    "TableSpec",
    "namespace_ddl",
    "table_ddl",
    "CompactionPolicy",
    "LifecycleContractError",
    "RetentionPolicy",
    "SnapshotManifest",
    "SnapshotReference",
    "build_snapshot_manifest",
    "orphan_file_dry_run",
    "plan_2025_hvfhs_schema_evolution",
    "pinned_snapshot_reference",
    "retention_dry_run",
    "should_compact",
]
