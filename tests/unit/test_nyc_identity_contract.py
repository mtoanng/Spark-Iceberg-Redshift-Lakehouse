"""Golden exact-row identity vectors shared by Python and Spark."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from etl.contracts.nyc_hvfhs_identity import (
    business_trip_key,
    identity_policy_version,
    row_id,
    spark_identity_expressions,
)


VECTOR_PATH = (
    Path(__file__).parents[1]
    / "fixtures"
    / "nyc_hvfhs"
    / "identity_golden_vectors.json"
)


def _vectors() -> list[dict[str, object]]:
    payload = json.loads(VECTOR_PATH.read_text(encoding="utf-8"))
    base = payload["base_2024"]
    return [
        {**vector, "record": {**base, **vector["overrides"]}}
        for vector in payload["vectors"]
    ]


def test_python_identity_matches_pinned_golden_vectors() -> None:
    vectors = _vectors()
    for vector in vectors:
        record, year = vector["record"], int(vector["year"])
        assert row_id(record, year) == vector["row_id"]
        assert business_trip_key(record) == vector["business_trip_key"]
        assert identity_policy_version(year) in {
            "nyc-hvfhv-row-v1-2024",
            "nyc-hvfhv-row-v1-2025",
        }

    by_name = {str(vector["name"]): vector for vector in vectors}
    assert (
        by_name["normal_2024"]["row_id"] == by_name["normalized_timestamps"]["row_id"]
    )
    for corrected in ("corrected_tips", "corrected_shared_flag"):
        assert (
            by_name[corrected]["business_trip_key"]
            == by_name["normal_2024"]["business_trip_key"]
        )
        assert by_name[corrected]["row_id"] != by_name["normal_2024"]["row_id"]

    base = vectors[0]["record"]
    assert row_id({**base, "bcf": 1}, 2024) != row_id(base, 2024)


def test_spark_identity_is_byte_for_byte_equal_to_python() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-identity-contract")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        for year in (2024, 2025):
            vectors = [vector for vector in _vectors() if vector["year"] == year]
            frame = spark.createDataFrame(
                [
                    {
                        **{
                            # Empty strings exercise the same canonical NULL token
                            # without creating all-null columns Spark cannot infer.
                            key: "" if value is None else str(value)
                            for key, value in vector["record"].items()
                        },
                        "_name": vector["name"],
                    }
                    for vector in vectors
                ]
            )
            exact, probable, policy = spark_identity_expressions(year)
            actual = {
                row["_name"]: (row["row_id"], row["business_trip_key"], row["policy"])
                for row in frame.select(
                    "_name",
                    exact.alias("row_id"),
                    probable.alias("business_trip_key"),
                    policy.alias("policy"),
                ).collect()
            }
            for vector in vectors:
                assert actual[vector["name"]] == (
                    vector["row_id"],
                    vector["business_trip_key"],
                    identity_policy_version(year),
                )
    finally:
        spark.stop()
