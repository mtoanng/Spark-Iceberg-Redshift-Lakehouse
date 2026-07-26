# Databricks notebook source
# Problem: Enrich canonical trips with two zone dimensions without losing unmatched drop-offs.
CATALOG = "<learner_catalog>"
SCHEMA = "lakehouse_learning"
DATA_DIR = "<path-to-learning_playground/data>"

# COMMAND ----------
from pyspark.sql import functions as F
trips = spark.read.option("header", True).csv(f"{DATA_DIR}/nyc_trips.csv")
zones = spark.read.option("header", True).csv(f"{DATA_DIR}/taxi_zones.csv")
display(zones)

# COMMAND ----------
# TODO: build canonical valid trips, then left join zones twice as pickup and
# dropoff dimensions. Keep the row where source_trip_id=14 and dropoff zone is null.
# Expected: 15 rows; source_trip_id=14 has pickup_zone='Financial District'.
# MAGIC %run ./solutions/04_join_taxi_zones
