"""Silver validation uses one deterministic priority."""

from __future__ import annotations

from etl.contracts.nyc_hvfhs_quality import REASON_PRIORITY, reason_code


BASE = {
    "request_datetime": "2024-01-01T08:00:00",
    "pickup_datetime": "2024-01-01T08:05:00",
    "dropoff_datetime": "2024-01-01T08:20:00",
    "PULocationID": 1,
    "DOLocationID": 2,
    "trip_miles": 1,
    "trip_time": 900,
    "base_passenger_fare": 10,
    "tolls": 0,
    "sales_tax": 0,
    "tips": 0,
    "driver_pay": 7,
}


def test_reason_priority_prefers_timeline_then_zone_then_numeric() -> None:
    assert REASON_PRIORITY[-1] == "DUPLICATE_ROW_ID"
    row = {
        **BASE,
        "pickup_datetime": None,
        "PULocationID": 999,
        "driver_pay": -1,
    }
    assert reason_code(row, {1, 2}) == "MISSING_OR_INVALID_TIMESTAMP"
    row = {**BASE, "PULocationID": 999, "driver_pay": -1}
    assert reason_code(row, {1, 2}) == "UNKNOWN_PICKUP_ZONE"
    row = {**BASE, "trip_miles": None, "driver_pay": -1}
    assert reason_code(row, {1, 2}) == "MISSING_OR_INVALID_NUMERIC"
