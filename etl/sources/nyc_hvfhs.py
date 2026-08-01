"""Pure-Python contracts for NYC TLC High Volume FHV monthly sources.

This module deliberately does not read S3, start Spark, or write Iceberg.
It defines the S3 input grain, required schema, and deterministic trip identity
that runtime ingestion must honour.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urlparse

from etl.contracts.nyc_hvfhs_identity import required_identity_columns

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


@dataclass(frozen=True)
class SourceFile:
    """Identity facts measured from a landed object or deterministic fixture."""

    source_year: int
    source_month: int
    source_uri: str
    source_checksum: str
    source_size_bytes: int


def _validate_year_month(year: int, month: int) -> None:
    if not _MONTH_PATTERN.fullmatch(str(year)):
        raise SourceContractError("year must be a four-digit year.")
    if not 1 <= month <= 12:
        raise SourceContractError("month must be between 1 and 12.")


def monthly_trip_filename(year: int, month: int) -> str:
    """Return the official TLC monthly HVFHV filename."""
    _validate_year_month(year, month)
    return f"fhvhv_tripdata_{year}-{month:02d}.parquet"


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
    if not re.fullmatch(r"[0-9a-f]{64}", source.source_checksum):
        raise SourceContractError(
            "landed source checksum must be a lowercase SHA-256 hex digest."
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


def stable_run_id(source: SourceFile) -> str:
    """Return the deterministic run key for a particular immutable source object."""

    identity = "\x1f".join(
        (
            source.source_uri,
            source.source_checksum,
            str(source.source_size_bytes),
            str(source.source_year),
            str(source.source_month),
        )
    )
    digest = sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"fhvhv-{source.source_year}-{source.source_month:02d}-{digest}"
