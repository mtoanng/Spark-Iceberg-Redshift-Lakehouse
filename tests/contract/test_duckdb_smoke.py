"""One end-to-end local smoke through the fixed query boundary."""

import duckdb
import pytest

from consumer import DuckDBGoldConsumer, QueryName
from tests.fixtures.gold_fixture import create_gold_fixture


def test_missing_parameter_failure_then_successful_rerun():
    connection = duckdb.connect(":memory:")
    create_gold_fixture(connection)
    consumer = DuckDBGoldConsumer(connection)

    with pytest.raises(duckdb.InvalidInputException, match="source_month"):
        consumer.run(
            QueryName.OPERATOR_TRIP_COUNT_AVERAGE_FARE,
            {"source_year": 2024},
        )

    rerun = consumer.run(
        QueryName.OPERATOR_TRIP_COUNT_AVERAGE_FARE,
        {"source_year": 2024, "source_month": 1},
    )
    assert [row[0] for row in rerun.rows] == ["HV0003", "HV0005"]
    connection.close()
