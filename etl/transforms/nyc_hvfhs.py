"""Bronze/Silver row contracts for NYC HVFHV data.

The functions in this module are intentionally pure Python so they can be
tested with small fixtures on a laptop. EMR Serverless uses the same identity
and reason-code contracts at runtime; this module neither starts Spark nor
writes canonical storage.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import urlparse

from etl.contracts.nyc_hvfhs_identity import identity_policy_version
from etl.contracts.nyc_hvfhs_quality import reason_code
from etl.sources.nyc_hvfhs import (
    SourceContractError,
    SourceFile,
    canonical_business_trip_key,
    canonical_row_id,
    validate_trip_schema,
)


METADATA_COLUMNS = (
    "_source_uri",
    "_source_file",
    "_source_year",
    "_source_month",
    "_source_checksum",
    "_ingestion_run_id",
    "_ingested_at",
    "row_id",
    "business_trip_key",
    "identity_policy_version",
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
        raise SourceContractError(
            f"Source URI does not contain a filename: {source_uri}"
        )
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
        "_source_uri": source.source_uri,
        "_source_file": _source_filename(source.source_uri),
        "_source_year": source.source_year,
        "_source_month": source.source_month,
        "_source_checksum": source.source_checksum,
        "_ingestion_run_id": ingestion_run_id,
        "_ingested_at": recorded_at,
    }
    rows = []
    for record in materialized:
        identity = {
            "row_id": canonical_row_id(record, source.source_year),
            "business_trip_key": canonical_business_trip_key(record),
            "identity_policy_version": identity_policy_version(source.source_year),
        }
        rows.append({**record, **metadata, **identity})
    return BronzeBatch(
        rows=tuple(rows),
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


def _quarantine_row(row: Mapping[str, object], code: str) -> dict[str, object]:
    return {**row, "reason_code": code}


def transform_silver(
    bronze_rows: Sequence[Mapping[str, object]],
    zone_ids: set[int],
    *,
    existing_row_ids: set[str] | None = None,
) -> SilverBatch:
    """Validate, deduplicate, and derive Silver trip rows with quarantine reasons."""
    seen_row_ids = set(existing_row_ids or set())
    silver_rows: list[dict[str, object]] = []
    quarantine_rows: list[dict[str, object]] = []

    for row in bronze_rows:
        reason = reason_code(row, zone_ids)
        exact_row_id = str(row["row_id"])
        if reason is None and exact_row_id in seen_row_ids:
            reason = "DUPLICATE_ROW_ID"
        if reason is not None:
            quarantine_rows.append(_quarantine_row(row, reason))
            continue

        seen_row_ids.add(exact_row_id)
        pickup = _as_datetime(row["pickup_datetime"])
        dropoff = _as_datetime(row["dropoff_datetime"])
        assert pickup is not None and dropoff is not None
        silver_rows.append(
            {
                "row_id": exact_row_id,
                "business_trip_key": row["business_trip_key"],
                "identity_policy_version": row["identity_policy_version"],
                "operator_code": row["hvfhs_license_num"],
                "request_datetime": row["request_datetime"],
                "pickup_datetime": row["pickup_datetime"],
                "dropoff_datetime": row["dropoff_datetime"],
                "pickup_zone_id": int(row["PULocationID"]),
                "dropoff_zone_id": int(row["DOLocationID"]),
                "trip_miles": float(row["trip_miles"]),
                "trip_time_seconds": int(float(row["trip_time"])),
                "passenger_fare": float(row["base_passenger_fare"]),
                "tolls": float(row["tolls"]),
                "sales_tax": float(row["sales_tax"]),
                "tips": float(row["tips"]),
                "driver_pay": float(row["driver_pay"]),
                "shared_request_flag": row["shared_request_flag"],
                "shared_match_flag": row["shared_match_flag"],
                "cbd_congestion_fee": (
                    None
                    if row.get("cbd_congestion_fee") is None
                    else float(row["cbd_congestion_fee"])
                ),
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
    result = Reconciliation(
        len(bronze.rows), len(silver.silver_rows), len(silver.quarantine_rows)
    )
    if not result.explained:
        raise SourceContractError(
            "Bronze reconciliation failed: every input row must be Silver or quarantine."
        )
    return result
