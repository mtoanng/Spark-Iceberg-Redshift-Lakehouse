"""Shared Silver validation priority for pure Python and Spark."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping


REASON_PRIORITY = (
    "MISSING_OR_INVALID_TIMESTAMP",
    "PICKUP_BEFORE_REQUEST",
    "DROPOFF_BEFORE_PICKUP",
    "INVALID_ZONE_ID",
    "UNKNOWN_PICKUP_ZONE",
    "UNKNOWN_DROPOFF_ZONE",
    "MISSING_OR_INVALID_NUMERIC",
    "NEGATIVE_TRIP_MILES",
    "NEGATIVE_TRIP_TIME",
    "NEGATIVE_PASSENGER_FARE",
    "NEGATIVE_TOLLS",
    "NEGATIVE_SALES_TAX",
    "NEGATIVE_TIPS",
    "NEGATIVE_DRIVER_PAY",
    "DUPLICATE_ROW_ID",
)
NUMERIC_REASON_COLUMNS = (
    ("trip_miles", "NEGATIVE_TRIP_MILES"),
    ("trip_time", "NEGATIVE_TRIP_TIME"),
    ("base_passenger_fare", "NEGATIVE_PASSENGER_FARE"),
    ("tolls", "NEGATIVE_TOLLS"),
    ("sales_tax", "NEGATIVE_SALES_TAX"),
    ("tips", "NEGATIVE_TIPS"),
    ("driver_pay", "NEGATIVE_DRIVER_PAY"),
)


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


def reason_code(row: Mapping[str, object], zone_ids: set[int]) -> str | None:
    requested = _as_datetime(row.get("request_datetime"))
    pickup = _as_datetime(row.get("pickup_datetime"))
    dropoff = _as_datetime(row.get("dropoff_datetime"))
    if requested is None or pickup is None or dropoff is None:
        return "MISSING_OR_INVALID_TIMESTAMP"
    if pickup < requested:
        return "PICKUP_BEFORE_REQUEST"
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
    parsed = [
        (column, code, _as_float(row.get(column)))
        for column, code in NUMERIC_REASON_COLUMNS
    ]
    if any(value is None for _, _, value in parsed):
        return "MISSING_OR_INVALID_NUMERIC"
    for _, code, value in parsed:
        if value is not None and value < 0:
            return code
    return None


def spark_reason_expression():
    """Build the Spark expression in exactly the same priority order."""

    from pyspark.sql.functions import col, when

    expression = when(
        col("request_datetime").isNull()
        | col("pickup_datetime").isNull()
        | col("dropoff_datetime").isNull(),
        "MISSING_OR_INVALID_TIMESTAMP",
    )
    expression = expression.when(
        col("pickup_datetime") < col("request_datetime"), "PICKUP_BEFORE_REQUEST"
    ).when(col("dropoff_datetime") < col("pickup_datetime"), "DROPOFF_BEFORE_PICKUP")
    expression = (
        expression.when(
            col("PULocationID").cast("int").isNull()
            | col("DOLocationID").cast("int").isNull(),
            "INVALID_ZONE_ID",
        )
        .when(col("pickup_zone_id").isNull(), "UNKNOWN_PICKUP_ZONE")
        .when(col("dropoff_zone_id").isNull(), "UNKNOWN_DROPOFF_ZONE")
    )
    expression = expression.when(
        col("trip_miles").cast("double").isNull()
        | col("trip_time").cast("double").isNull()
        | col("base_passenger_fare").cast("double").isNull()
        | col("tolls").cast("double").isNull()
        | col("sales_tax").cast("double").isNull()
        | col("tips").cast("double").isNull()
        | col("driver_pay").cast("double").isNull(),
        "MISSING_OR_INVALID_NUMERIC",
    )
    for column, code in NUMERIC_REASON_COLUMNS:
        expression = expression.when(col(column).cast("double") < 0, code)
    return expression
