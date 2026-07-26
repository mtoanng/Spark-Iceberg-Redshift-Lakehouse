"""Pure-Python contracts for NYC TLC High Volume FHV monthly sources.

This module deliberately does not read S3, start Spark, or write Iceberg.
Those responsibilities begin in later phases.  It defines the source grain,
required source schema, deterministic trip identity, and manifest decisions
that later ingestion code must honour.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
import re
from typing import Iterable, Mapping
from urllib.parse import urlparse

from etl.contracts.nyc_hvfhs_identity import (
    business_trip_key,
    required_identity_columns,
    row_id,
)

NYC_TLC_TRIP_DATA_BASE_URI = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_TLC_TAXI_ZONE_URI = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)
DEFAULT_SOURCE_YEAR = 2024
DEFAULT_SOURCE_MONTH = 1

_MONTH_PATTERN = re.compile(r"^20\d{2}$")

# These are the source-level fields needed for the Phase 2 validation and the
# Phase 3 Gold contract. More TLC columns may be preserved in Bronze later.
BASE_REQUIRED_TRIP_COLUMNS = frozenset(
    {
        "hvfhs_license_num",
        "dispatching_base_num",
        "request_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tolls",
        "sales_tax",
        "tips",
        "driver_pay",
        "shared_request_flag",
        "shared_match_flag",
    }
)
CBD_CONGESTION_FEE_COLUMN = "cbd_congestion_fee"


class SourceContractError(ValueError):
    """Raised when a source does not meet the declared contract."""


class SourceStatus(StrEnum):
    DISCOVERED = "discovered"
    PROCESSED = "processed"


class ManifestAction(StrEnum):
    PROCESS_NEW = "process_new"
    SKIP_IDENTICAL_PROCESSED_SOURCE = "skip_identical_processed_source"
    PROCESS_FORCED_RETRY = "process_forced_retry"
    BLOCK_CHANGED_CHECKSUM = "block_changed_checksum"


@dataclass(frozen=True)
class SourceFile:
    """Identity facts measured from a local source or deterministic fixture."""

    source_year: int
    source_month: int
    source_uri: str
    source_checksum: str
    source_size_bytes: int


@dataclass(frozen=True)
class SourceManifestEntry:
    """The Phase 1 in-memory representation of one monthly source manifest row."""

    source_year: int
    source_month: int
    source_uri: str
    source_checksum: str
    source_size_bytes: int
    status: SourceStatus
    first_seen_at: datetime
    processed_at: datetime | None
    ingestion_run_id: str | None

    @classmethod
    def discovered(
        cls, source: SourceFile, seen_at: datetime | None = None
    ) -> "SourceManifestEntry":
        return cls(
            source_year=source.source_year,
            source_month=source.source_month,
            source_uri=source.source_uri,
            source_checksum=source.source_checksum,
            source_size_bytes=source.source_size_bytes,
            status=SourceStatus.DISCOVERED,
            first_seen_at=seen_at or datetime.now(timezone.utc),
            processed_at=None,
            ingestion_run_id=None,
        )

    def processed(
        self, run_id: str, processed_at: datetime | None = None
    ) -> "SourceManifestEntry":
        if not run_id:
            raise SourceContractError(
                "A processed source requires an ingestion_run_id."
            )
        return SourceManifestEntry(
            source_year=self.source_year,
            source_month=self.source_month,
            source_uri=self.source_uri,
            source_checksum=self.source_checksum,
            source_size_bytes=self.source_size_bytes,
            status=SourceStatus.PROCESSED,
            first_seen_at=self.first_seen_at,
            processed_at=processed_at or datetime.now(timezone.utc),
            ingestion_run_id=run_id,
        )


def _validate_year_month(year: int, month: int) -> None:
    if not _MONTH_PATTERN.fullmatch(str(year)):
        raise SourceContractError("year must be a four-digit year.")
    if not 1 <= month <= 12:
        raise SourceContractError("month must be between 1 and 12.")


def monthly_trip_filename(year: int, month: int) -> str:
    """Return the official TLC monthly HVFHV filename."""
    _validate_year_month(year, month)
    return f"fhvhv_tripdata_{year}-{month:02d}.parquet"


def monthly_trip_uri(year: int, month: int) -> str:
    """Return the official TLC monthly HVFHV URI without downloading it."""
    return f"{NYC_TLC_TRIP_DATA_BASE_URI}/{monthly_trip_filename(year, month)}"


def validate_landed_source(source: SourceFile) -> None:
    """Require one immutable, checksum-pinned monthly object in S3 landing."""

    _validate_year_month(source.source_year, source.source_month)
    parsed = urlparse(source.source_uri)
    expected_name = monthly_trip_filename(source.source_year, source.source_month)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or Path(parsed.path).name != expected_name
    ):
        raise SourceContractError(
            f"landed source must be an s3:// URI ending in {expected_name}."
        )
    if not re.fullmatch(r"[0-9a-fA-F]{64}", source.source_checksum):
        raise SourceContractError(
            "landed source checksum must be a SHA-256 hex digest."
        )
    if source.source_size_bytes <= 0:
        raise SourceContractError("landed source size must be greater than zero bytes.")


def required_trip_columns(year: int) -> frozenset[str]:
    """Return required source columns; 2025+ requires congestion-fee support."""
    if year < 2019:
        raise SourceContractError("HVFHV source files are supported from 2019 onward.")
    required = BASE_REQUIRED_TRIP_COLUMNS | required_identity_columns(year)
    if year >= 2025:
        return required | {CBD_CONGESTION_FEE_COLUMN}
    return required


def validate_trip_schema(columns: Iterable[str], year: int) -> None:
    """Reject a source schema missing any Phase 1 required source columns."""
    present = set(columns)
    missing = sorted(required_trip_columns(year) - present)
    if missing:
        raise SourceContractError(
            f"Missing required HVFHV source columns: {', '.join(missing)}"
        )


def inspect_local_source(
    path: Path, year: int, month: int, source_uri: str | None = None
) -> SourceFile:
    """Calculate immutable local source identity without interpreting its contents."""
    _validate_year_month(year, month)
    if not path.is_file():
        raise SourceContractError(f"Source file does not exist: {path}")

    digest = sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)

    return SourceFile(
        source_year=year,
        source_month=month,
        source_uri=source_uri or path.resolve().as_uri(),
        source_checksum=digest.hexdigest(),
        source_size_bytes=path.stat().st_size,
    )


def canonical_row_id(record: Mapping[str, object], year: int) -> str:
    """Produce the policy-versioned exact-row identity."""

    try:
        return row_id(record, year)
    except ValueError as error:
        raise SourceContractError(str(error)) from error


def canonical_business_trip_key(record: Mapping[str, object]) -> str:
    """Produce the probable-trip analytical key; never use it for deduplication."""

    try:
        return business_trip_key(record)
    except ValueError as error:
        raise SourceContractError(str(error)) from error


def stable_run_id(source: SourceFile) -> str:
    """Return the deterministic run key for a particular immutable source object."""
    return f"fhvhv-{source.source_year}-{source.source_month:02d}-{source.source_checksum[:16]}"


def manifest_decision(
    existing: SourceManifestEntry | None,
    candidate: SourceFile,
    *,
    force: bool = False,
) -> ManifestAction:
    """Decide whether a candidate source may proceed without mutating state.

    A changed checksum is blocked even when ``force`` is true: accepting a new
    object requires a separately reviewed checksum-change workflow in a later
    phase. Force is only a deliberate retry of the same immutable source.
    """
    if existing is None:
        return ManifestAction.PROCESS_NEW

    same_location = (
        existing.source_year == candidate.source_year
        and existing.source_month == candidate.source_month
        and existing.source_uri == candidate.source_uri
    )
    if not same_location or existing.source_checksum != candidate.source_checksum:
        return ManifestAction.BLOCK_CHANGED_CHECKSUM
    if existing.status is SourceStatus.PROCESSED:
        return (
            ManifestAction.PROCESS_FORCED_RETRY
            if force
            else ManifestAction.SKIP_IDENTICAL_PROCESSED_SOURCE
        )
    return ManifestAction.PROCESS_NEW
