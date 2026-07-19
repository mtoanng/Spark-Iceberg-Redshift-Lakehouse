"""Bronze/Silver row contracts for NYC HVFHV data.

The functions in this module are intentionally pure Python so they can be
tested with small fixtures on a laptop. Glue jobs use the same column and
reason-code contract at production scale; this module neither starts Spark nor
writes canonical storage.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import urlparse

from etl.sources.nyc_hvfhs import SourceContractError, SourceFile, canonical_trip_id, validate_trip_schema


METADATA_COLUMNS = (
    "_source_file",
    "_source_year",
    "_source_month",
    "_source_checksum",
    "_ingestion_run_id",
    "_ingested_at",
)


@dataclass(frozen=True)
class BronzeBatch:
    rows: tuple[dict[str, object], ...]
    source: SourceFile
    ingestion_run_id: str


@dataclass(frozen=True)
class SilverBatch:
    silver_rows: tuple[dict[str, object], ...]
    quarantine_rows: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class Reconciliation:
    bronze_count: int
    silver_count: int
    quarantine_count: int

    @property
    def explained(self) -> bool:
        return self.bronze_count == self.silver_count + self.quarantine_count


def _source_filename(source_uri: str) -> str:
    parsed = urlparse(source_uri)
    filename = Path(parsed.path).name
    if not filename:
        raise SourceContractError(f"Source URI does not contain a filename: {source_uri}")
    return filename


def bronze_records(
    records: Iterable[Mapping[str, object]],
    source: SourceFile,
    ingestion_run_id: str,
    *,
    ingested_at: datetime | None = None,
) -> BronzeBatch:
    """Copy source records unchanged and attach only required Bronze metadata."""
    if not ingestion_run_id:
        raise SourceContractError("Bronze ingestion requires an ingestion_run_id.")
    materialized = [dict(record) for record in records]
    if materialized:
        validate_trip_schema(materialized[0].keys(), source.source_year)

    recorded_at = ingested_at or datetime.now(timezone.utc)
    metadata = {
        "_source_file": _source_filename(source.source_uri),
        "_source_year": source.source_year,
        "_source_month": source.source_month,
        "_source_checksum": source.source_checksum,
        "_ingestion_run_id": ingestion_run_id,
        "_ingested_at": recorded_at,
    }
    return BronzeBatch(
        rows=tuple({**record, **metadata} for record in materialized),
        source=source,
        ingestion_run_id=ingestion_run_id,
    )


def load_zone_ids(path: Path) -> set[int]:
    """Load only lookup IDs for deterministic local Silver validation."""
    with path.open(newline="", encoding="utf-8") as zone_file:
        reader = csv.DictReader(zone_file)
        if "LocationID" not in (reader.fieldnames or []):
            raise SourceContractError("Taxi zone lookup requires LocationID.")
        return {int(row["LocationID"]) for row in reader}


def _as_datetime(value: object) -> datetime | None:
    if value is None or not str(value).strip():
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _as_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reason_code(row: Mapping[str, object], zone_ids: set[int]) -> str | None:
    pickup = _as_datetime(row.get("pickup_datetime"))
    dropoff = _as_datetime(row.get("dropoff_datetime"))
    if pickup is None or dropoff is None:
        return "MISSING_OR_INVALID_TIMESTAMP"
    if dropoff < pickup:
        return "DROPOFF_BEFORE_PICKUP"
    try:
        pickup_zone = int(row["PULocationID"])
        dropoff_zone = int(row["DOLocationID"])
    except (KeyError, TypeError, ValueError):
        return "INVALID_ZONE_ID"
    if pickup_zone not in zone_ids:
        return "UNKNOWN_PICKUP_ZONE"
    if dropoff_zone not in zone_ids:
        return "UNKNOWN_DROPOFF_ZONE"
    for column, code in (
        ("trip_miles", "NEGATIVE_TRIP_MILES"),
        ("trip_time", "NEGATIVE_TRIP_TIME"),
        ("base_passenger_fare", "NEGATIVE_PASSENGER_FARE"),
        ("driver_pay", "NEGATIVE_DRIVER_PAY"),
    ):
        metric = _as_float(row.get(column))
        if metric is None:
            return f"INVALID_{column.upper()}"
        if metric < 0:
            return code
    return None


def _quarantine_row(row: Mapping[str, object], reason_code: str, trip_id: str | None) -> dict[str, object]:
    return {**row, "trip_id": trip_id, "reason_code": reason_code}


def transform_silver(
    bronze_rows: Sequence[Mapping[str, object]],
    zone_ids: set[int],
    *,
    existing_trip_ids: set[str] | None = None,
) -> SilverBatch:
    """Validate, deduplicate, and derive Silver trip rows with quarantine reasons."""
    seen_trip_ids = set(existing_trip_ids or set())
    silver_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []

    for row in bronze_rows:
        reason = _reason_code(row, zone_ids)
        trip_id: str | None = None
        try:
            trip_id = canonical_trip_id(row)
        except SourceContractError:
            reason = reason or "MISSING_TRIP_IDENTITY_FIELD"

        if reason is None and trip_id in seen_trip_ids:
            reason = "DUPLICATE_TRIP_ID"
        if reason is not None:
            quarantine_rows.append(_quarantine_row(row, reason, trip_id))
            continue

        assert trip_id is not None  # guarded by the identity-field validation above
        seen_trip_ids.add(trip_id)
        pickup = _as_datetime(row["pickup_datetime"])
        dropoff = _as_datetime(row["dropoff_datetime"])
        assert pickup is not None and dropoff is not None
        silver_rows.append(
            {
                "trip_id": trip_id,
                "operator_code": row["hvfhs_license_num"],
                "request_datetime": row["request_datetime"],
                "pickup_datetime": row["pickup_datetime"],
                "dropoff_datetime": row["dropoff_datetime"],
                "pickup_zone_id": int(row["PULocationID"]),
                "dropoff_zone_id": int(row["DOLocationID"]),
                "trip_miles": float(row["trip_miles"]),
                "trip_time_seconds": int(float(row["trip_time"])),
                "passenger_fare": float(row["base_passenger_fare"]),
                "tolls": float(row.get("tolls") or 0),
                "sales_tax": float(row.get("sales_tax") or 0),
                "tips": float(row.get("tips") or 0),
                "driver_pay": float(row["driver_pay"]),
                "shared_request_flag": row["shared_request_flag"],
                "shared_match_flag": row["shared_match_flag"],
                "source_year": row["_source_year"],
                "source_month": row["_source_month"],
                "ingestion_run_id": row["_ingestion_run_id"],
                "trip_duration_minutes": (dropoff - pickup).total_seconds() / 60,
                "pickup_date": pickup.date().isoformat(),
                "pickup_hour": pickup.hour,
            }
        )

    return SilverBatch(tuple(silver_rows), tuple(quarantine_rows))


def reconcile(bronze: BronzeBatch, silver: SilverBatch) -> Reconciliation:
    """Return a count reconciliation and reject unexplained input rows."""
    result = Reconciliation(len(bronze.rows), len(silver.silver_rows), len(silver.quarantine_rows))
    if not result.explained:
        raise SourceContractError(
            "Bronze reconciliation failed: every input row must be Silver or quarantine."
        )
    return result
