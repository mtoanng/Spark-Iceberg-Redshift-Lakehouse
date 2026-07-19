from decimal import Decimal

import duckdb
import pytest

from consumer import DuckDBGoldConsumer, QueryName
from consumer.duckdb_consumer import _validate_s3_location
from tests.fixtures.gold_fixture import create_gold_fixture


@pytest.fixture()
def consumer():
    connection = duckdb.connect(":memory:")
    create_gold_fixture(connection)
    yield DuckDBGoldConsumer(connection)
    connection.close()


def test_hourly_pickups_by_zone(consumer):
    result = consumer.run(QueryName.HOURLY_PICKUPS_BY_ZONE)

    assert result.columns == (
        "pickup_date_key",
        "pickup_hour",
        "pickup_zone_id",
        "borough",
        "zone_name",
        "trip_count",
    )
    assert result.rows == (
        (20240115, 8, 1, "Queens", "Astoria", 2),
        (20240115, 8, 2, "Manhattan", "Midtown", 1),
        (20240115, 9, 3, "Brooklyn", "Downtown Brooklyn", 1),
    )


def test_operator_trip_count_and_average_fare(consumer):
    result = consumer.run(
        QueryName.OPERATOR_TRIP_COUNT_AVERAGE_FARE,
        {"source_year": 2024, "source_month": 1},
    )

    assert result.rows == (
        ("HV0003", 3, Decimal("20.00")),
        ("HV0005", 1, Decimal("12.00")),
    )


def test_top_pickup_zones_for_selected_month(consumer):
    result = consumer.run(
        QueryName.TOP_PICKUP_ZONES,
        {"source_year": 2024, "source_month": 1, "limit": 2},
    )

    assert result.rows == (
        (1, "Queens", "Astoria", 2),
        (2, "Manhattan", "Midtown", 1),
    )


def test_fare_and_driver_pay_reconciliation(consumer):
    result = consumer.run(
        QueryName.FARE_DRIVER_PAY_RECONCILIATION,
        {"source_year": 2024, "source_month": 1},
    )

    assert result.rows == (
        (2024, 1, 4, Decimal("72.00"), Decimal("49.00"), Decimal("23.00")),
    )


def test_explain_analyze_returns_a_physical_plan(consumer):
    result = consumer.run(
        QueryName.EXPLAIN_FILTERED_TRIPS,
        {"source_year": 2024, "source_month": 1},
    )

    assert "analyzed_plan" in {str(value) for row in result.rows for value in row}
    assert any("TABLE_SCAN" in str(value) for row in result.rows for value in row)


def test_arbitrary_sql_is_rejected(consumer):
    with pytest.raises(TypeError, match="arbitrary SQL"):
        consumer.run("SELECT * FROM gold.fct_trips")


def test_iceberg_registration_requires_exact_gold_contract():
    with pytest.raises(ValueError, match="Gold table contract mismatch"):
        DuckDBGoldConsumer.from_iceberg_locations({"fct_trips": "s3://bucket/table"})


def test_iceberg_registration_rejects_unsafe_location():
    with pytest.raises(ValueError, match="safe s3"):
        _validate_s3_location("s3://bucket/fct'; DROP TABLE x;--")
