"""Single source of truth for NYC HVFHV exact-row and business identity.

The canonical string representation is deliberately simple enough to reproduce
with Python and Spark SQL. Ingestion metadata and Silver-derived fields never
participate in either identity.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Mapping


NULL_TOKEN = "<NULL>"
FIELD_SEPARATOR = "\x1f"
IDENTITY_POLICY_2024 = "nyc-hvfhv-row-v1-2024"
IDENTITY_POLICY_2025 = "nyc-hvfhv-row-v1-2025"

TIMESTAMP_COLUMNS = frozenset(
    {
        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime",
    }
)
INTEGER_COLUMNS = frozenset({"PULocationID", "DOLocationID", "trip_time"})
NUMERIC_COLUMNS = frozenset(
    {
        "trip_miles",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "cbd_congestion_fee",
    }
)

IDENTITY_COLUMNS_2024 = (
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag",
)
IDENTITY_COLUMNS_2025 = IDENTITY_COLUMNS_2024 + ("cbd_congestion_fee",)
BUSINESS_KEY_COLUMNS = (
    "hvfhs_license_num",
    "dispatching_base_num",
    "request_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)


def identity_policy_version(year: int) -> str:
    return IDENTITY_POLICY_2025 if year >= 2025 else IDENTITY_POLICY_2024


def identity_columns(year: int) -> tuple[str, ...]:
    return IDENTITY_COLUMNS_2025 if year >= 2025 else IDENTITY_COLUMNS_2024


def required_identity_columns(year: int) -> frozenset[str]:
    return frozenset(identity_columns(year))


def _timestamp_text(value: object) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")


def _decimal_text(value: object) -> str:
    try:
        decimal_value = Decimal(str(value).strip())
    except InvalidOperation as error:
        raise ValueError(f"Invalid numeric identity value: {value!r}") from error
    if not decimal_value.is_finite():
        raise ValueError(f"Non-finite numeric identity value: {value!r}")
    return format(decimal_value.quantize(Decimal("0.000001")), "f")


def canonical_value(column: str, value: object) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return NULL_TOKEN
    if column in TIMESTAMP_COLUMNS:
        return _timestamp_text(value)
    if column in INTEGER_COLUMNS:
        return str(int(Decimal(str(value).strip())))
    if column in NUMERIC_COLUMNS:
        return _decimal_text(value)
    return str(value).strip()


def _hash(record: Mapping[str, object], columns: tuple[str, ...], policy: str) -> str:
    values = [policy]
    values.extend(canonical_value(column, record.get(column)) for column in columns)
    return sha256(FIELD_SEPARATOR.join(values).encode("utf-8")).hexdigest()


def row_id(record: Mapping[str, object], year: int) -> str:
    """Return the exact-row SHA-256 for the declared source-year policy."""

    missing = [column for column in identity_columns(year) if column not in record]
    if missing:
        raise ValueError("Missing row identity columns: " + ", ".join(missing))
    return _hash(record, identity_columns(year), identity_policy_version(year))


def business_trip_key(record: Mapping[str, object]) -> str:
    """Return a probable-trip key that must never drive exact deduplication."""

    missing = [column for column in BUSINESS_KEY_COLUMNS if column not in record]
    if missing:
        raise ValueError("Missing business key columns: " + ", ".join(missing))
    return _hash(record, BUSINESS_KEY_COLUMNS, "nyc-hvfhv-business-v1")


def spark_canonical_value(column: str):
    """Build the Spark expression matching :func:`canonical_value`."""

    from pyspark.sql.functions import col, date_format, lit, trim, when

    source = col(column)
    missing = source.isNull() | (trim(source.cast("string")) == "")
    if column in TIMESTAMP_COLUMNS:
        normalized = date_format(
            source.cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        )
    elif column in INTEGER_COLUMNS:
        normalized = source.cast("decimal(38,0)").cast("string")
    elif column in NUMERIC_COLUMNS:
        normalized = source.cast("decimal(38,6)").cast("string")
    else:
        normalized = trim(source.cast("string"))
    return when(missing, lit(NULL_TOKEN)).otherwise(normalized)


def spark_identity_expressions(year: int):
    """Return Spark Columns for row_id, business_trip_key, and policy version."""

    from pyspark.sql.functions import concat_ws, lit, sha2

    policy = identity_policy_version(year)
    exact_values = [lit(policy)] + [
        spark_canonical_value(column) for column in identity_columns(year)
    ]
    business_values = [lit("nyc-hvfhv-business-v1")] + [
        spark_canonical_value(column) for column in BUSINESS_KEY_COLUMNS
    ]
    return (
        sha2(concat_ws(FIELD_SEPARATOR, *exact_values), 256),
        sha2(concat_ws(FIELD_SEPARATOR, *business_values), 256),
        lit(policy),
    )
