"""Pure, durable-source manifest state machine.

The Iceberg ``ops.source_run_manifest`` table is the persistence target.  This
module deliberately contains no Spark or AWS calls so retry semantics can be
tested deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum

from etl.contracts.nyc_hvfhs_identity import identity_policy_version
from etl.sources.nyc_hvfhs import SourceContractError, SourceFile, stable_run_id


class RunStatus(StrEnum):
    DISCOVERED = "discovered"
    BRONZE_PUBLISHED = "bronze_published"
    SILVER_PUBLISHED = "silver_published"
    RECONCILED = "reconciled"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceRunManifest:
    source_uri: str
    source_checksum: str
    source_size_bytes: int
    source_year: int
    source_month: int
    ingestion_run_id: str
    identity_policy_version: str
    run_status: RunStatus
    first_seen_at: datetime
    updated_at: datetime
    bronze_row_count: int = 0
    silver_row_count: int = 0
    quarantine_row_count: int = 0
    gold_row_count: int = 0
    bronze_snapshot_id: str | None = None
    silver_snapshot_id: str | None = None
    quarantine_snapshot_id: str | None = None
    failure_stage: str | None = None
    failure_message: str | None = None
    completed_at: datetime | None = None
    publication_status: str | None = None
    publication_manifest_uri: str | None = None
    published_at: datetime | None = None

    @classmethod
    def discovered(
        cls, source: SourceFile, seen_at: datetime | None = None
    ) -> "SourceRunManifest":
        now = seen_at or datetime.now(timezone.utc)
        return cls(
            source_uri=source.source_uri,
            source_checksum=source.source_checksum,
            source_size_bytes=source.source_size_bytes,
            source_year=source.source_year,
            source_month=source.source_month,
            ingestion_run_id=stable_run_id(source),
            identity_policy_version=identity_policy_version(source.source_year),
            run_status=RunStatus.DISCOVERED,
            first_seen_at=now,
            updated_at=now,
        )

    def _transition(
        self, status: RunStatus, *, at: datetime | None = None, **changes: object
    ) -> "SourceRunManifest":
        return replace(
            self,
            run_status=status,
            updated_at=at or datetime.now(timezone.utc),
            **changes,
        )

    def bronze_published(
        self, row_count: int, *, at: datetime | None = None
    ) -> "SourceRunManifest":
        if row_count < 0:
            raise SourceContractError("Bronze row count cannot be negative.")
        return self._transition(
            RunStatus.BRONZE_PUBLISHED, at=at, bronze_row_count=row_count
        )

    def silver_published(
        self, silver_count: int, quarantine_count: int, *, at: datetime | None = None
    ) -> "SourceRunManifest":
        if self.run_status is not RunStatus.BRONZE_PUBLISHED:
            raise SourceContractError("Canonical Silver publication requires Bronze.")
        if (
            min(silver_count, quarantine_count) < 0
            or self.bronze_row_count != silver_count + quarantine_count
        ):
            raise SourceContractError(
                "Bronze rows must reconcile to Silver plus quarantine rows."
            )
        now = at or datetime.now(timezone.utc)
        return self._transition(
            RunStatus.SILVER_PUBLISHED,
            at=now,
            silver_row_count=silver_count,
            quarantine_row_count=quarantine_count,
            completed_at=now,
        )

    def reconciled(
        self, gold_count: int, *, at: datetime | None = None
    ) -> "SourceRunManifest":
        if self.run_status is not RunStatus.SILVER_PUBLISHED:
            raise SourceContractError("Gold reconciliation requires published Silver.")
        if gold_count != self.silver_row_count:
            raise SourceContractError(
                "Gold fact rows must equal canonical Silver rows."
            )
        return self._transition(
            RunStatus.RECONCILED,
            at=at,
            gold_row_count=gold_count,
            publication_status="pending",
        )

    def published(
        self, manifest_uri: str, *, at: datetime | None = None
    ) -> "SourceRunManifest":
        if self.run_status is not RunStatus.RECONCILED:
            raise SourceContractError("Publication requires a reconciled run.")
        if not manifest_uri:
            raise SourceContractError("Publication requires a durable manifest URI.")
        now = at or datetime.now(timezone.utc)
        return self._transition(
            RunStatus.PUBLISHED,
            at=now,
            publication_status="published",
            publication_manifest_uri=manifest_uri,
            published_at=now,
            completed_at=now,
        )

    def failed(
        self, stage: str, message: str, *, at: datetime | None = None
    ) -> "SourceRunManifest":
        if not stage or not message:
            raise SourceContractError(
                "A failed run must include stage and failure information."
            )
        return self._transition(
            RunStatus.FAILED, at=at, failure_stage=stage, failure_message=message
        )


def retry_is_safe(
    existing: SourceRunManifest | None, candidate: SourceFile, *, force: bool
) -> bool:
    """Return whether a monthly run may write its canonical partition.

    The same immutable source can be retried only when forced.  A checksum or
    URI change is never accepted through this path.
    """
    if existing is None:
        return True
    identity_matches = (
        existing.source_year == candidate.source_year
        and existing.source_month == candidate.source_month
        and existing.source_uri == candidate.source_uri
        and existing.source_checksum == candidate.source_checksum
        and existing.source_size_bytes == candidate.source_size_bytes
    )
    if not identity_matches:
        return False
    canonical_complete = existing.run_status in {
        RunStatus.SILVER_PUBLISHED,
        RunStatus.RECONCILED,
        RunStatus.PUBLISHED,
    }
    return not canonical_complete or force
